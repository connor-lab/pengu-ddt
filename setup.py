import setuptools
import pip

with open("VERSION", 'r') as v:
    version = v.readline()

try: # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
    from pip.req import parse_requirements


if int(pip.__version__.split(".")[0]) >= 6:
    install_reqs = parse_requirements('requirements.txt', session=False)
else:
    install_reqs = parse_requirements('requirements.txt')

install_requires = [str(ir.req) for ir in install_reqs]

setuptools.setup(
    name='ddt',
    version=version,
    author="Matt Bull",
    author_email="Matthew.Bull@Wales.nhs.uk",
    description="A package for interacting with the DIGEST database",
    packages=setuptools.find_packages(),
    scripts=["ddt.py"],
    install_requires=install_requires
    )