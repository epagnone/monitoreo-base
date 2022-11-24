# Monitoreos

Este repositorio contiene el código para desarrollar las funciones de monitoreo tanto del _performance_ como de la estabilidad de los modelos de clasificación **binaria**. 
A su vez, tiene funciones que permiten insertar la salida de las funciones anteriormente mencionadas (_dataframes_ de _pyspark_) en las tablas de [monitoreo de modelos](https://docucio.telecom.com.ar/pages/viewpage.action?pageId=47908003) 
Las funciones son las siguientes:

## calcular_vdi_cuantitativas_baseline
Calcula el **_baseline_** de las variables **cuantitativas** para un modelo y fecha en particular.

    Inputs:
    -------
        - id: Nombre del campo que identifica el 'id' del modelo por ejemplo el campo 'linea' o el campo 'id_suscripcion'.
        - fecha_foto: fecha en la que se requiere ver la distribución de las variables yyyymmdd.
        - variables: lista de variables a monitorear. No más de 20 variables.
        - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo.
        - nombre_modelo: Nombre del modelo a monitorear el performance.
        - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). 
                    Por defecto 'sdb_datamining'.
    Outputs:
    -------
        - df_vdi_cuanti_bl: Dataframe con la distribución baseline de las variables

## calcular_vdi_cualitativas_baseline
Calcula el **_baseline_** de las variables **cualitativas** para un modelo y fecha en particular.

    Inputs:
    -------
        - id: Nombre del campo que identifica el 'id' del modelo por ejemplo el campo 'linea' o el campo 'id_suscripcion'.
        - fecha_foto: fecha en la que se requiere ver la distribución de las variables yyyymmdd.
        - variables: lista de variables a monitorear. No más de 20 variables.
        - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo.
        - nombre_modelo: Nombre del modelo a monitorear el performance.
        - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). 
                    Por defecto 'sdb_datamining'.
    Outputs:
    -------
        - df_vdi_cuali_bl: Dataframe con la distribución baseline de las variables

## calcular_performance_baseline
Calcula las métricas de _performance_ para un periodo de _baseline_ para un modelo y fecha en particular.

    Inputs:
    -------
        - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo a monitorear el performance.
        - nombre_modelo: Nombre del modelo a monitorear el performance.
        - query: String que contiene el join entre el target y el score del modelo. (Respetar los 'as' en los querys)
        - fecha_score: Fecha en la que se requiere verificar el performance del modelo. Está en formato yyyymmdd.
        - cant_bines: Cantidad de bines que se creará con la distribución del score. Aplica solo para el baseline. Por defecto 20.
        - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.
    Outputs:
    --------
        - df_res_bines, df_res_metricas, df_psi: Dataframes en spark que contienen la tabla de performance, la tabla con las métricas de performance y el baseline del psi, respectivamente.
        
## calcular_performance_actual
Calcula las métricas de _performance_ para un modelo y fecha en particular.

    Inputs:
    -------
        - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo a monitorear el performance.
        - nombre_modelo: Nombre del modelo a monitorear el performance.
        - query: String que contiene el join entre el target y el score del modelo. (Respetar los 'as' en los querys)
        - fecha_score: Fecha en la que se requiere verificar el performance del modelo. Está en formato yyyymmdd.
        - cant_bines: Cantidad de bines que se creará con la distribución del score. Aplica solo para el baseline. Por defecto 20.
        - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.

    Outputs:
    --------
        - df_res_bines, df_res_metricas: Dataframes en spark que contienen la tabla de performance, la tabla con las métricas de performance.
        
## calcular_psi
Calcula el psi para un modelo y fecha en particular.

    Inputs:
    -------
        - id: Nombre del campo que identifica el 'id' del modelo por ejemplo el campo 'linea' o el campo 'id_suscripcion'.
        - score: Nombre del campo que identifica el 'score' del modelo por ejemplo el campo 'score_capro'. 
        - fecha_foto: Fecha en la que se requiere verificar la estabilidad de la población scoreada. Está en formato yyyymmdd.
        - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo a monitorear la estabilidad.
        - nombre_modelo: Nombre del modelo a monitorear la estabilidad.
        - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto sdb_datamining
    Outputs:
    -------
        - df_ind_PSI: Dataframe con el calculo de contribution_to_index por bin.
        
## 	calcular_vdi_cualitativas
Calcula el vdi de las variables cualitativas para una fecha y modelo en particular.

    Inputs:
    -------
        - id: Nombre del campo que identifica el 'id' del modelo por ejemplo el campo 'linea' o el campo 'id_suscripcion'.
        - score: Nombre del campo que identifica el 'score' del modelo por ejemplo el campo 'score_capro'. 
        - variables: lista de variables cualitativas a monitorear (No más de 20 variables.)
        - fecha_foto: Fecha en la que se requiere verificar la estabilidad de la población scoreada. Está en formato yyyymmdd.
        - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo a monitorear la estabilidad.
        - nombre_modelo: Nombre del modelo a monitorear la estabilidad.
        - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto sdb_datamining
    Outputs:
    -------
        - df_vdi_cuali_move: dataframe de spark con el calculo del vdi de cada una de las variables cualitativas.
        
## calcular_vdi_cuantitativas
Calcula el vdi de las variables cuantitativas para una fecha y modelo en particular.

    Inputs:
    -------
        - id: Nombre del campo que identifica el 'id' del modelo por ejemplo el campo 'linea' o el campo 'id_suscripcion'.
        - score: Nombre del campo que identifica el 'score' del modelo por ejemplo el campo 'score_capro'. 
        - variables: lista de variables cuantitativas a monitorear. (hasta 20 variables que deben estar definidas en el baseline)
        - fecha_foto: Fecha en la que se requiere verificar la estabilidad de la población scoreada. Está en formato yyyymmdd.
        - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo a monitorear la estabilidad.
        - nombre_modelo: Nombre del modelo a monitorear la estabilidad.
        - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto sdb_datamining 
    Outputs:
    --------
        - df_vdi_cuanti_move: dataframe en spark con el calculo del vdi de cada una de las variables cuantitativas.
        
## insertar_vdi_cuantitativas_baseline
Inserta en hadoop el _dataframe_ de _spark_ que tiene el _baseline_ de las variables cuantitativas.

    Inputs:
    -------
        - df_vdi_cuanti_bl: Dataframe de spark con el baseline de las variables cuantitativas a insertar en hadoop.
        - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.
        

## insertar_vdi_cualitativas_baseline
Inserta en hadoop el _dataframe_ de _spark_ que tiene el _baseline_ de las variables cualitativas.

    Inputs:
    -------
        - df_vdi_cuali_bl: Dataframe de spark con el baseline de las variables cualitativas a insertar en hadoop.
        - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.

## insertar_performance_baseline
Inserta en hadoop el _dataframe_ de _spark_ que tiene el _performance_ _baseline_ de un modelo en particular.

    Inputs:
    -------
        - df_res_bines: Dataframe en spark que contienen la tabla de performance
        - df_res_metricas: Dataframe en spark que contienen la tabla con las métricas de performance
        - df_psi: Dataframe que tiene el baseline del psi.
        - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.

## insertar_performance_actual
Inserta en hadoop el _dataframe_ de _spark_ que tiene el _performance_ para un modelo y fecha en particular.

    Inputs:
    -------
        - df_res_bines: Dataframe en spark que contienen la tabla de performance
        - df_res_metricas: Dataframe en spark que contienen la tabla con las métricas de performance
        - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.

## insertar_psi
Inserta en hadoop el _dataframe_ de _spark_ que tiene el psi para un modelo y fecha en particular.

    Inputs:
    -------
        - df_psi: Dataframe que tiene el psi del modelo
        - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.

## insertar_vdi
Inserta en hadoop el _dataframe_ de _spark_ que tiene el vdi de las variables cuantitativas o cualitativas.

    Inputs:
    -------
        - df_vdi: Dataframe que tiene el el vdi de las variables cualitativas del modelo.
        - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.