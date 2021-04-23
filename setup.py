from setuptools import setup, find_packages

setup(
    name='pypipeflow',
    version='1.0.1',
    author='Awayne',
    author_email='everAwayne@gmail.com',
    description='a simple library that you can set up service based on different kinds of MQ',
    long_description_content_type='text/markdown',
    url='https://github.com/everAwayne/pipeflow',
    packages=['pipeflow'],
    include_package_data=True,
    python_requires='>=3.5',
    zip_safe=False,
    classifiers=[
        'Framework :: AsyncIO',
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy'
    ],
    install_requires=[
        'aiohttp',
        'pynsq>=0.9.0',
        'redis',
        'aio-pika'
    ],
    extras_require={
        'uvloop': ['uvloop']
    }
)
