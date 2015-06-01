"""
SQLAlchemy-Flux-Serializer
-------------

Serialize sqlalchemy models into flux-friendly json
"""
from setuptools import setup


setup(
    name='SQLAlchemy-Flux-Serializer',
    version='0.0.1',
    url='https://github.com/alexkuz/sqlalchemy-flux-serializer',
    license='MIT',
    author='Alexander Kuznetsov',
    author_email='alexkuz@gmail.com',
    description='Serialize sqlalchemy models into flux-friendly json',
    long_description=__doc__,
    packages=['flux_serializer'],
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=[
        'Flask',
        'SQLAlchemy'
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
