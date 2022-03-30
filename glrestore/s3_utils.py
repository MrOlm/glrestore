import boto3

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

    # If STANDARD, StorageClass wont be in this
    if ('ResponseMetadata' in re) & ('StorageClass' not in re):
        sclass = 'STANDARD'
    else:
        sclass = re['StorageClass']

    return sclass

def object_glacerized(s3_loc, **kwargs):
    """
    Check if an object is in aws s3 glacier, and if so, return True. Else, return False.
    """
    return get_object_storage_class(s3_loc, **kwargs) == 'GLACIER'