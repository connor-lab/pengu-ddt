import setuptools

with open("VERSION", 'r') as v:
    version = v.readline()

setuptools.setup(
    name='ddt',
    version=version,
    author="Matt Bull",
    author_email="Matthew.Bull@Wales.nhs.uk",
    description="A package for interacting with the DIGEST database",
    packages=setuptools.find_packages(),
    scripts=["ddt.py"]
    )