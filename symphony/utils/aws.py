from symphony.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, USE_MODIN, AWS_REGION
from symphony.exceptions import UtilsException
import boto3
from botocore.exceptions import ClientError
from typing import Union, List, Optional, Any, Tuple
import gzip
import pickle
from s3path import S3Path
from io import BytesIO, TextIOWrapper

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


def get_s3_resource() -> boto3.resources.base.ServiceResource:
    """
    Returns a boto3 S3 resource

    :return: S3 resource
    """
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    return session.resource('s3')


def get_s3_path(bucket: str, folder_or_folders: Union[str, List[str]]) -> str:
    """
    Creates an s3 path from a bucket name

    :param bucket: Bucket name
    :param folder_or_folders: single folder or list of them
    :return:
    """
    base_path = "s3://" + bucket + "/"
    if isinstance(folder_or_folders, str):
        base_path += folder_or_folders.strip("/")
    elif isinstance(folder_or_folders, list):
        base_path += "/".join(folder_or_folders)
    else:
        raise UtilsException(f"Unknown type for folder {folder_or_folders}")
    return base_path


def s3_file_exists(s3_path: str, s3_resource: Optional[boto3.resources.base.ServiceResource] = None) -> bool:
    """
    Check if an S3 object exists

    :param s3_path: The canonical S3:// path
    :param s3_resource: optional resource
    :return: True if the file exists, false otherwise
    """

    resource, bucket_name, key = __get_resource_bucket_key(s3_resource, s3_path)
    object = resource.Object(bucket_name, key)
    try:
        object.load()
        return True
    except ClientError as e:
        pass
    except Exception as e:
        raise e

    return False


def s3_create_folder(s3_path: str, s3_resource: Optional[boto3.resources.base.ServiceResource] = None) -> None:
    """
    Creates a 'folder' in S3 by PUTing an empty key

    :param s3_path: The folder path
    :param s3_resource: Optional instantiated S3 resource
    :return: None
    """
    resource, bucket_name, key = __get_resource_bucket_key(s3_resource, s3_path)

    if not key.endswith("/"):
        key += "/"

    bucket = resource.Bucket(bucket_name)

    if not s3_file_exists(s3_path, s3_resource=resource):
        bucket.put_object(Key=key)
    return


def s3_upload_python_object(s3_path: str, object: Any, s3_resource: Optional[Any] = None) -> None:
    """
    Uploads arbitrary python object to S3. Must be picklable

    :param s3_path: Fully qualified S3 path
    :param object: The object
    :param s3_resource: Optional S3 resource
    :return: None
    """
    resource, bucket_name, key = __get_resource_bucket_key(s3_resource, s3_path)
    serialized_object = pickle.dumps(object)
    bucket = resource.Bucket(bucket_name)
    bucket.put_object(Key=key, Body=serialized_object)
    return


def s3_download_python_object(s3_path: str, s3_resource: Optional[Any] = None) -> Any:
    """
    Downloads and unpickles S3 object

    :param s3_path: Fully qualified S3 path
    :param s3_resource: Optional S3 resource
    :return: None
    """
    resource, bucket_name, key = __get_resource_bucket_key(s3_resource, s3_path)
    with BytesIO() as data:
        resource.Bucket(bucket_name).download_fileobj(key, data)
        data.seek(0)
        return pickle.load(data)


def upload_dataframe_to_s3(dataframe: pd.DataFrame, s3_path: str, s3_resource: Optional[Any] = None,
                           index: Optional[bool] = False) -> None:
    """
    Uploads a dataframe to an S3 path

    :param dataframe: Dataframe to upload
    :param s3_path: S3 canonical path
    :param s3_resource: Optional boto resource
    :param index: Optionally add index
    :return: None
    :raises UtilsException: If unable to upload
    """
    resource, bucket_name, key = __get_resource_bucket_key(s3_resource, s3_path)

    if s3_path.endswith(".gz"):
        gz_buffer = BytesIO()
        with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
            dataframe.to_csv(TextIOWrapper(gz_file, 'utf8'), index=index)

        resource.Bucket(bucket_name).put_object(Key=key, Body=gz_buffer.getvalue())
    elif s3_path.endswith(".csv"):
        dataframe.to_csv(s3_path, index=False)
    else:
        raise UtilsException(f"Unimplemented upload for filetype: {s3_path}")
    return


def s3_list_folders(s3_path: str, s3_resource: Optional[Any] = None) -> List[str]:
    """
    Lists contents of S3 bucket at path

    :param s3_path: Path to list contents of
    :param s3_resource: Optional S3 resource
    :return: List of contents
    """
    if not s3_path.endswith("/"):
        s3_path += "/"

    resource, bucket_name, key = __get_resource_bucket_key(s3_resource, s3_path)
    bucket = resource.Bucket(bucket_name)
    resp = bucket.meta.client.list_objects_v2(Bucket=bucket_name, Prefix=key, MaxKeys=100, Delimiter='/')
    folders = [prefix['Prefix'] for prefix in resp['CommonPrefixes']]
    start = f"s3://{bucket_name}/"
    canonical_paths = [start + folder for folder in folders]
    return canonical_paths


def get_dataframe_from_s3(s3_path: str, index_column: Optional[str] = False,
                          parse_dates: Optional[bool] = False) -> pd.DataFrame:
    """
    Reads a dataframe from an S3 path

    :param s3_path: Path to object
    :param index_column: Optional index column
    :param parse_dates: Optionally parse dates
    :return: The dataframe
    """

    kwargs = {
        "index_col": index_column
    }
    if s3_path.endswith(".gz"):
        kwargs["compression"] = 'gzip'
    if index_column:
        kwargs["index_col"] = index_column
    kwargs["parse_dates"] = parse_dates

    return pd.read_csv(s3_path, **kwargs)


def __get_resource_bucket_key(resource: Union[None, boto3.resources.base.ServiceResource], s3_path: str) -> Tuple[
    boto3.resources.base.ServiceResource, str, str]:
    """
    Helper method for common arguments
    :param resource: Resource passed to function
    :param s3_path: S3 path passed to function
    :return: Tuple of resource, bucket name, key
    """
    resource = resource
    if isinstance(resource, type(None)):
        resource = get_s3_resource()

    bucket_name = s3_path.replace("s3://", "").split("/")[0]
    key = "/".join(s3_path.replace("s3://", "").split("/")[1:])
    return resource, bucket_name, key
