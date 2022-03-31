import boto3
import logging

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

def get_object_storage_class(s3_loc, **kwargs):
    """
    Return the storage class of an s3_loc
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

    return sclass, rclass

def glacier_status(s3_loc, **kwargs):
    """
    Check if an object is in aws s3 glacier, and if so, return True. Else, return False.
    """
    sclass, rclass = get_object_storage_class(s3_loc, **kwargs)

    # Object is not in glacier at all
    if sclass != 'GLACIER':
        return 'no-glacier'

    # Object is in glacier with no active restored
    elif rclass is False:
        return 'glacier-no-restore'

    # Object is restored in glacier
    elif rclass == 'restored':
        return 'glacier-restored'

    # Object is being actively restored
    else:
        assert rclass == 'restoring'
        return 'glacier-restoring'

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