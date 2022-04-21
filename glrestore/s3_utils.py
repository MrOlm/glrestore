import boto3
import logging
import datetime

import pandas as pd

from collections import defaultdict

def get_boto3_client(**kwargs):
    """
    The point of this is really to handle the "profile" option when setting up boto3
    """
    # This means you already have a client made
    if 'client' in kwargs:
        return kwargs.get('client')

    # This means I'll make you a new client
    if 'profile' in kwargs:
        profile_name = kwargs.get('profile')
        session = boto3.session.Session(profile_name=profile_name)
    else:
        session = boto3.session.Session()
    return session.client("s3")

def get_bucket_key(s3_loc):
    """
    From a full s3 location, return the bucket and key
    """
    if not s3_loc.startswith('s3://'):
        raise Exception(f"{s3_loc} is not a properly formatted key")

    bucket = s3_loc.split('/')[2]
    key = '/'.join(s3_loc.split('/')[3:])

    return bucket, key

def get_object_storage_class(s3_loc, extra_info=False, **kwargs):
    """
    Return the storage class and restoring status of an s3_loc

    If "extra_info", return a dictionary including creation date, last modified date, and size
    """
    client = get_boto3_client(**kwargs)
    bucket, key = get_bucket_key(s3_loc)
    re = client.head_object(Bucket=bucket, Key=key)

    # Get the storage class. If STANDARD, StorageClass won't be in this
    if ('ResponseMetadata' in re) & ('StorageClass' not in re):
        sclass = 'STANDARD'
    else:
        sclass = re['StorageClass']

    # Get the restore status. If not restored, 'Restore' won't be in this
    if 'Restore' not in re:
        rclass = False
    else:
        r = re['Restore']
        active_restore = 'ongoing-request="true"' in r
        # if not active_restore:
        #     expiry_date = r.split('expiry-date=\"')[1]

        if active_restore:
            rclass = 'restoring'
        else:
            rclass = 'restored'

    if extra_info:
        retd = {'storage_class':sclass, 'restore_status':rclass}
        re_header = re['ResponseMetadata']['HTTPHeaders']
        for name, rname in zip(['LastModified', 'size_bytes'], ['last-modified', 'content-length']):
            if rname in re_header:
                val = re_header[rname]

                if name in ['LastModified']:
                    val = datetime.datetime.strptime(val, "%a, %d %b %Y %H:%M:%S %Z")

                retd[name] = val
        return retd

    else:
        return sclass, rclass

def glacier_status(s3_loc, **kwargs):
    """
    Check if an object is in aws s3 glacier, and if so, return True. Else, return False.
    """
    sclass, rclass = get_object_storage_class(s3_loc, **kwargs)

    # Object is not in glacier at all
    if sclass not in ['GLACIER', 'DEEP_ARCHIVE']:
        return 'no-glacier'

    # Object is in glacier with no active restored
    elif rclass is False:
        if sclass == 'GLACIER':
            return 'glacier-no-restore'
        elif sclass == 'DEEP_ARCHIVE':
            return 'deep-glacier-no-restore'
        else:
            print(f"WHAT!! {sclass}")
            assert False, sclass

    # Object is restored in glacier
    elif rclass == 'restored':
        return 'glacier-restored'

    # Object is being actively restored
    else:
        assert rclass == 'restoring'
        return 'glacier-restoring'

def classify_glacier_objects(files, **kwargs):
    """
    Classify all files
    """
    table = defaultdict(list)
    for f in files:
        table['file'].append(f)

        rdic = get_object_storage_class(f, extra_info=True, **kwargs)
        for n, v in rdic.items():
            table[n].append(v)

    db = pd.DataFrame(table)
    db['size_bytes'] = db['size_bytes'].astype(float)
    return db

def restore_file(f, **kwargs):
    """
    Restore "f" using the parameters in kwargs
    """
    client = get_boto3_client(**kwargs)

    obucket, okey = get_bucket_key(f)

    response = client.restore_object(
        Bucket=obucket,
        Key=okey,
        RestoreRequest={
            'Days': kwargs.get('days'),
            'GlacierJobParameters': {
             'Tier': str(kwargs.get('speed'))}})

    if kwargs.get('debug', False):
        logging.debug(response)