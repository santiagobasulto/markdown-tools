import uuid
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import click

from .uploaders import upload_relative_images

PRINT_LOCK = threading.Lock()


def upload_s3(file, output, location, boto3_client, **uploader_kwargs):
    abs_output = output.format(filename=file.stem)

    absolute_path = file.with_name(abs_output)
    if location is not None:
        location_path = Path(location)
        assert location_path.exists()
        absolute_path = location_path / abs_output

    s3_base_key = uploader_kwargs["s3_base_key"].format(
        filename=file.stem,
        parent_0=file.resolve().parents[0].stem,
        random_hex=str(uuid.uuid4()).split("-")[0])

    result = upload_relative_images(
        file,
        absolute_path,
        "s3",
        override=uploader_kwargs["s3_override"],
        **{
            "s3_client": boto3_client,
            "s3_bucket": uploader_kwargs["s3_bucket"],
            "s3_relative_path": s3_base_key,
            "s3_ACL": uploader_kwargs.get("s3_acl"),
            "s3_cloudfront_domain": uploader_kwargs.get("s3_cloudfront_domain"),
            "s3_cache_control": uploader_kwargs.get("s3_cache_control"),
        },
    )
    return result


def process_s3(files, output, location, concurrency, verbose, **uploader_kwargs):
    # Required params
    required_params = ["s3_bucket", "s3_base_key"]
    assert all([uploader_kwargs.get(param) for param in required_params])

    optional_credential_kwargs = {
        "s3_profile_name",
        "s3_aws_access_key_id",
        "s3_aws_secret_access_key",
        "s3_aws_session_token",
        "s3_region_name",
    }
    session_kwargs = {
        optional_kwarg.replace("s3_", ""): uploader_kwargs.get(optional_kwarg)
        for optional_kwarg in optional_credential_kwargs
        if optional_kwarg in uploader_kwargs
    }

    boto3_session = boto3.Session(**session_kwargs)
    boto3_client = boto3_session.client("s3")
    # for file in files:
    #     upload_s3(file, output, location, boto3_client, **uploader_kwargs)

    max_workers = min((concurrency or 1), len(files))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(
                upload_s3, file, output, location, boto3_client, **uploader_kwargs
            ): file
            for file in files
        }
        success = []
        error = []
        for f in as_completed(futures):
            location = futures[f]
            with PRINT_LOCK:
                if f.exception():
                    print(f"FAILED: {location}")
                    print(repr(f.exception()))
                    error.append(f)
                else:
                    print(f"SUCCESS: {location}")
                    success.append(f)

        print("\n\n")
        print("Successful jobs:")
        for future in success:
            location = futures[future]
            print(f"\t{location}")
        else:
            print("\t-")

        print("-" * 30)
        print("Errored jobs:")
        for future in error:
            location = futures[future]
            print(f"\t**{location}**: {future.exception()}")


UPLOADERS = {"s3": process_s3, "imgur": None}


@click.group()
def cli():
    pass


@cli.command()
@click.argument(
    "path", type=click.Path(exists=True),
)
@click.option(
    "-p",
    "--pattern",
    default="**/*.md",
    help="If `path` is a directory, this represents a Pathlib.glob pattern to scan markdown files with",
)
@click.option(
    "-e",
    "--exclude",
    default="absolute",
    help="Exclude any files with the given word in the name, to avoid recursive uploads it defaults to 'absolute'. Pass blank to remove exclusions",
)
@click.option(
    "-o",
    "--output",
    default="{filename}.absolute.md",
    help="Filename to use for the resulting absolute file. Pattern can contain {filename}.",
)
@click.option(
    "-l",
    "--location",
    default=None,
    help="Where to store the absolute file. If omitted, it's stored in the same location as the relative file",
)
@click.option(
    "-x",
    "--concurrency",
    type=int,
    default=1,
    help="Number of concurrent threads to spawn",
)
@click.option(
    "-u",
    "--uploader",
    type=click.Choice(["s3", "imgur"], case_sensitive=False),
    default="s3",
    help="The uploader to use: S3|Imgur (case insensitve)",
)
@click.option("-v", "--verbose", count=True)
@click.option("--s3_profile_name")
@click.option("--s3_aws_access_key_id")
@click.option("--s3_aws_secret_access_key")
@click.option("--s3_aws_session_token")
@click.option("--s3_region_name")
@click.option("--s3_bucket")
@click.option("--s3_base_key")
@click.option("--s3_ACL", default="private")
@click.option("--s3_cloudfront_domain")
@click.option("--s3_cache_control", default="public, max-age=31536000")
@click.option("--s3_override", type=bool, default=False)
def rel_to_abs(
    path,
    pattern,
    exclude,
    output,
    location,
    concurrency,
    uploader,
    verbose,
    **uploader_kwargs,
):
    path = Path(path)
    files = [path]
    if path.is_dir():
        files = [file for file in path.glob(pattern) if exclude not in file.name]
    assert uploader in {"s3", "imgur"}
    uploader_callable = UPLOADERS[uploader]
    uploader_callable(files, output, location, concurrency, verbose, **uploader_kwargs)


if __name__ == "__main__":
    cli()
