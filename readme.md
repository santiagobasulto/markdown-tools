API:

```
# Template params: {filename}, {stem?}
md_tools rel_to_abs . --pattern "*.md" -o "{filename}.abs.md" --location None -x 4 --uploader s3 -bucket -aws_profile -vvv

# --pattern: the Pathlib pattern search (default "**/*.md")
# -o output pattern
# --location: where to store the abs file. if Not passed (None), same place as original file. If passed to default location.
# -x concurrency
# --uploader s3|imgur
# -v verbosity

# s3_bucket
# s3_base_key: base key for the image, replacement variables are: {filename}, {parent_0}, {random_hex}
# s3_ACL
# s3_cloudfront_domain
# s3_cache_control
# override
```

Future:

```bash
$ md_tools rel-to-abs . --pattern "*.md" -o "
```
