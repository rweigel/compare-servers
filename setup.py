from setuptools import setup, find_packages

install_requires = [
    "deepdiff",
    "urllib3",
    "requests",
    "hapiclient",
    "requests_cache==1.2"
]

try:
  # Will work if utilrsw was already installed, for example via pip install -e .
  import utilrsw
except:
  install_requires.append("utilrsw @ git+https://github.com/rweigel/utilrsw")

setup(
    name='compare-servers',
    version='0.0.1',
    author='Bob Weigel',
    author_email='rweigel@gmu.edu',
    packages=find_packages(),
    license='LICENSE.txt',
    description='Compare output of two HAPI servers.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    install_requires=install_requires
)
