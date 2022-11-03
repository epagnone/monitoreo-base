# Monitoreos

Este repositorio contiene el código para desarrollar las funciones de monitoreo tanto del _performance_ como de la estabilidad de los modelos de clasificación. 
A su vez, tiene funciones que permiten insertar la salida de las funciones anteriormente mencionadas (_dataframes_ de _pyspark_) en las tablas de [monitoreo de modelos](https://docucio.telecom.com.ar/pages/viewpage.action?pageId=47908003) 
Las funciones son las siguientes:

## vdi
Función que calcula el baseline de la distribución de las variables cuantitativas y cualitativas.

        _Inputs_:
        -------
            - id: Nombre del campo que identifica el 'id' del modelo por ejemplo el campo 'linea' o el campo 'id_suscripcion'.
            - fecha_foto: fecha en la que se requiere ver la distribución de las variables yyyymmdd.
            - variables: lista de variables a monitorear. No más de 10 variables.
            - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo.
            - nombre_modelo: Nombre del modelo a monitorear el performance.
            - tipo_variables: Indica si el grupo de variables es cualitativa o cuantitativa. Por defecto 'CUALITATIVAS'.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.
            
        _Outputs_:
        -------
            - df_vdi_bl: Dataframe con la distribución baseline de las variables

## insert_vdi
Función que inseta en hadoop la tabla del baseline de vdi.

        _Inputs_:
        -------
            - df_vdi_bl: Dataframe de spark a insertar en hadoop.
            - tipo_variables: Indica si el grupo de variables es cualitativa o cuantitativa. Por defecto 'CUALITATIVAS'.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.