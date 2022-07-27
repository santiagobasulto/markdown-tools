import re
import mimetypes
import hashlib
import threading
from pathlib import Path
import urllib.parse

import requests

import boto3
from botocore.exceptions import ClientError


PATTERN_FULL = '(?:!\[(?P<alt_text>.*?)\]\((?P<filename>.*?)\))'
PATTERN_FNAME = '(?:!\[(?:.*?)\]\((?P<filename>.*?)\))'

S3_CLIENT_CREATION_LOCK = threading.Lock()

class Uploader:
    def upload_image(self, image_path, override=False):
        raise NotImplementedError()


class S3Uploader(Uploader):
    def __init__(self, s3_bucket, s3_relative_path, s3_ACL=None, s3_cloudfront_domain=None, s3_cache_control=None, s3_client=None):
        self.bucket = s3_bucket
        self.relative_path = s3_relative_path.rstrip('/').lstrip('/')
        self.s3_acl = s3_ACL
        self.cloudfront_domain = s3_cloudfront_domain
        self.cache_control = s3_cache_control

        if self.cloudfront_domain:
            assert not self.cloudfront_domain.startswith('http://'), "Invalid cloudfront domain"
            assert not self.cloudfront_domain.startswith('https://'), "Invalid cloudfront domain"
        if s3_client:
            self.client = s3_client
        else:
            with S3_CLIENT_CREATION_LOCK:
                self.client = boto3.client('s3')

    def upload_image(self, image_path, override=False, validate_etag=True):
        dns = self.cloudfront_domain or f'{self.bucket}.s3.amazonaws.com'
        p = Path(image_path)

        kwargs = {}
        if self.cache_control:
            kwargs['CacheControl'] = self.cache_control
        if self.s3_acl:
            kwargs['ACL'] = self.s3_acl

        key = f'{self.relative_path}/{p.name}'
        url = f"https://{dns}/{urllib.parse.quote(key)}"
        if not override:
            try:
                resp = self.client.head_object(Bucket=self.bucket, Key=key)
                remote_etag = resp.get('ETag', '').strip('"')
                if not remote_etag or not validate_etag:
                    return url

                with p.open('rb') as fp:
                    local_etag = hashlib.md5(fp.read()).hexdigest()
                    if local_etag == remote_etag:
                        return url
            except ClientError as exc:
                if exc.response['Error']['Code'] != "404":
                    raise exc
        content_type, _ = mimetypes.guess_type(image_path)
        if content_type:
            kwargs['ContentType'] = content_type

        with p.open('rb') as fp:
            self.client.put_object(
                Body=fp.read(),
                Bucket=self.bucket,
                Key=key,
                **kwargs
            )
        return url


class ImgurUploader(Uploader):
    def __init__(self, imgur_access_token):
        self.access_token = imgur_access_token

    def upload_image(self, image_path, override=False):
        p = Path(image_path)
        headers = {"Authorization": f"Bearer {self.access_token}"}
        with p.open('rb') as fp:
            files = {'image': fp}
            resp = requests.post(
                'https://api.imgur.com/3/image',
                headers=headers, files=files)
        resp.raise_for_status()
        return resp.json()['data']['link']


UPLOADERS = {
    's3': S3Uploader,
    'imgur': ImgurUploader
}

def upload_relative_images(original_path, output_path, uploader, override=False, **uploader_kwargs):
    """Reads a markdown file, finds all the images and uploads them using `uploader`.
    The result is a new file under `output_path`. Provide specific parameters
    for the uploader with `uploader_kwargs`.

    Parameters
    ----------
    original_path: str, a valid filesystem path
        The path of the markdown file used to transform.
    output_path: str, a valid filesystem path
        The path of where the resulting markdown file will be stored.
        WARNING! This file will be overwritten.
    uploader: str, a choice of uploaders
        The uploader to use. Currently only supported in the `UPLOADERS` variable.
    override: bool
        Passed to the uploader, if the image should be overridden or not.
        It's responsability of the uploader to respect this flag.
    **uploader_kwargs: keyword arguments
        Everything else will be passed to the Uploader at the moment of initialization.
    """
    is_relative = lambda url: not bool(urllib.parse.urlparse(url).netloc)
    UploaderClass = UPLOADERS[uploader]
    uploader = UploaderClass(**uploader_kwargs)

    original_path = Path(original_path)
    base_path = original_path.parent
    pattern = re.compile(PATTERN_FNAME)

    with original_path.open() as fp:
        content = fp.read()
        image_relative_paths = {url for url in pattern.findall(content) if is_relative(url)}

    image_mapping = {
        image_relative: (base_path / urllib.parse.unquote(image_relative)) for image_relative in image_relative_paths
    }
    missing_images = [str(abs_path) for _, abs_path in image_mapping.items() if not abs_path.exists()]
    if missing_images:
        raise ValueError(f'Missing images: {",".join(missing_images)}')

    image_results = {
        relative_path: uploader.upload_image(abs_path, override)
        for relative_path, abs_path in image_mapping.items()
    }

    for relative_path, upload_path in image_results.items():
        content = content.replace(relative_path, upload_path)
    with open(output_path, 'w') as fp:
        fp.write(content)

    return image_results


CMD_REQUIRED_ARGUMENTS = {
    's3': ['s3_bucket', 's3_relative_path']
}

if __name__ == "__main__":
    import argparse
    import pathlib
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('input', type=pathlib.Path, help='A path to the markdown with relative images to transform')
    parser.add_argument('output', type=pathlib.Path, help='A path to store the output of the process')
    parser.add_argument('-u', '--uploader', choices=['s3', 'imgur'], required=True)
    parser.add_argument('-o', '--override', action='store_const', const=True, default=False)

    # S3 specific params
    parser.add_argument('--s3-bucket')
    parser.add_argument('--s3-relative-path', help="Where to store the images within the bucket. A key prefix.")
    parser.add_argument('--s3-acl', default='private')
    parser.add_argument('--s3-cf-domain', help="The cloudfront domain to use instead of S3's default URL")
    parser.add_argument('--s3-cache-control')

    # Imgur specific params
    parser.add_argument('--imgur-access-token')

    args = parser.parse_args()
    assert args.input.exists(), "Markdown file doesn't exist"
    assert args.input != args.output
    assert all([bool(getattr(args, arg)) for arg in CMD_REQUIRED_ARGUMENTS[args.uploader]]), "Missing arguments"

    if args.uploader == 's3':
        results = upload_relative_images(
            args.input,
            args.output,
            's3',
            override=args.override,
            s3_bucket=args.s3_bucket,
            s3_relative_path=args.s3_relative_path,
            s3_ACL=args.s3_acl,
            cloudfront_domain=args.s3_cf_domain,
            cache_control=args.s3_cache_control,
        )
    else:
        results = results = upload_relative_images(
            args.input,
            args.output,
            'imgur',
            override=args.override,
            imgur_access_token=args.imgur_access_token,
        )
    print('\n')
    print('-' * 60)
    print(f"Replaced {len(results)} images")
