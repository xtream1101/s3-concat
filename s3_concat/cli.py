import logging
import argparse
from . import S3Concat

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)


def cli():
    parser = argparse.ArgumentParser(description='Convert data files')
    parser.add_argument("--bucket",
                        help="base bucket to use",
                        required=True,
                        )
    parser.add_argument("--folder",
                        help="folder whose contents should be combined",
                        required=True,
                        )
    parser.add_argument("--output",
                        help=("output location for resulting merged files,"
                              " relative to the specified base bucket"),
                        required=True,
                        )
    parser.add_argument("--filesize",
                        help=("Min filesize of the concatenated files"
                              " in [B,KB,MB,GB,TB]. e.x. 5.2GB"),
                        required=True,
                        )
    parser.add_argument("--small-parts-threads",
                        type=int,
                        help=("[Advanced Usage] Number of threads to"
                              " download small parts with. (Default: 1)"),
                        default=1)
    args = parser.parse_args()

    job = S3Concat(args.bucket, args.output, args.filesize)
    job.add_files(args.folder)
    job.concat(small_parts_threads=args.small_parts_threads)
