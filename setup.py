import subprocess
from setuptools import setup, find_packages
import medgen

setup(
    name = 'medgen',
    version = medgen.__version__,
    description = 'MedGen is the NCBI primary source for Medical Genomics information',
    long_description = 'Modules in medgen.* provide access to the databases and concepts found in medgen-mysql.',
    url = 'https://bitbucket.org/locusdevelopment/medgen-python',
    author = 'Invitae, Inc.',
    maintainer = 'BioMed',
    author_email = 'biomed@invitae.com',
    maintainer_email = 'biomed@invitae.com',
    license = 'Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0)',
    packages = find_packages(),
    package_data={'medgen': ['config/*.ini']},
    install_requires = [
        'pymysql',
        #'metapub',  # pip install -e /path/to/metapub-py3k
        'pyrfc3339',
        ],
    )

