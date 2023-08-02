from botocore.exceptions import ClientError

def upload_file(
        file_path,
        s3_client,
        s3_resource,
        bucket,
        object_name,
        make_public=False):
    try:
        response = s3_client.upload_file(str(file_path), bucket, object_name)
        print(response)
        if make_public:
            object_acl = s3_resource.ObjectAcl(bucket, object_name)
            resp = object_acl.put(ACL="public-read")
            return True
        else:
            return False
    except ClientError as e:
        print(e)
        return False
    return True
