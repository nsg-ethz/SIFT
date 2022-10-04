from setuptools import setup

setup(
    name='sift',
    version='0.0.1',
    description='SIFT',
    py_modules=['sift'],
    package_dir={'': 'lib'},
    scripts=[
        'bin/sift_queue',
        'bin/sift_cli',
        'bin/sift_dispatcher',
        'bin/sift_fetcher',
        'bin/sift_test_fetcher',
        'bin/sift_stitch',
    ]
)
