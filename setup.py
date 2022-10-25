import pathlib
from setuptools import find_packages, setup

HERE = pathlib.Path(__file__).parent

VERSION = '0.0.1' #Version
PACKAGE_NAME = 'monitoreo' #Debe coincidir con el nombre de la carpeta 
AUTHOR = 'Eduardo Pagnone' #Modificar 
AUTHOR_EMAIL = 'eduardopagnone@gmail.com' #Modificar 
URL = 'https://github.com/epagnone/monitoreo-base' #Modificar 

LICENSE = 'MIT' #Tipo de licencia
DESCRIPTION = 'Descripcion' #Descripción corta
LONG_DESCRIPTION = (HERE/"README.md").read_text(encoding='utf-8') #Referencia al README 
LONG_DESC_TYPE = "text/markdown"


#Dependencias
INSTALL_REQUIRES = []

setup(
    name=PACKAGE_NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type=LONG_DESC_TYPE,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    url=URL,
    install_requires=INSTALL_REQUIRES,
    license=LICENSE,
    packages=find_packages(),
    include_package_data=True
)