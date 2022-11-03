# Monitoreos

Este repositorio contiene el código para desarrollar las funciones de monitoreo tanto del _performance_ como de la estabilidad de los modelos de clasificación. 
A su vez, tiene funciones que permiten insertar la salida de las funciones anteriormente mencionadas (_dataframes_ de _pyspark_) en las tablas de [monitoreo de modelos](https://docucio.telecom.com.ar/pages/viewpage.action?pageId=47908003) 
Las funciones son las siguientes:

## vdi
Función que calcula el baseline de la distribución de las variables cuantitativas y cualitativas.

        Inputs:
        -------
            - id: Nombre del campo que identifica el 'id' del modelo por ejemplo el campo 'linea' o el campo 'id_suscripcion'.
            - fecha_foto: fecha en la que se requiere ver la distribución de las variables yyyymmdd.
            - variables: lista de variables a monitorear. No más de 10 variables.
            - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo.
            - nombre_modelo: Nombre del modelo a monitorear el performance.
            - tipo_variables: Indica si el grupo de variables es cualitativa o cuantitativa. Por defecto 'CUALITATIVAS'.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.
            
        Outputs:
        -------
            - df_vdi_bl: Dataframe con la distribución baseline de las variables

## insert_vdi
Función que inseta en hadoop la tabla del baseline de vdi.

        _Inputs_:
        -------
            - df_vdi_bl: Dataframe de spark a insertar en hadoop.
            - tipo_variables: Indica si el grupo de variables es cualitativa o cuantitativa. Por defecto 'CUALITATIVAS'.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.
            
## performance
Función que calcula las métricas de performance para los modelos de clasificación binaria, tales como Information Value, Kolmogorov Smirnov (KS), Gini, AUC. 
        También, crea la tabla de performance en donde podemos apreciar métricas tales como Lift Acumulado, % Captura, % Conversión.
        En el caso de que el parámetro 'tipo' tenga valor 'BASELINE'se creará un dataframe que tenga el psi del periodo especificado.
        Importante: Enviamos como parámetro el query que joinea el target con el score, es necesario usar alias en este query para identificar correctamente el 'id', 'target' y el 'score'. 
        Ejemplo:
        
        select distinct 
                            a.linea as id, 
                            a.score as score,
                            coalesce(tg_incobr90_m6,0) as target
                     from data_lake_analytics.abt_incobred_m a
                     left join data_lake_analytics.ft_abonos_m b 
                     on (a.linea=b.linea and b.periodo='202104')
                     where a.periodo=202010 
         
        Inputs:
        -------
            - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo a monitorear el performance.
            - nombre_modelo: Nombre del modelo a monitorear el performance.
            - query: String que contiene el join entre el target y el score del modelo. (Respetar los 'as' en los querys)
            - fecha_score: Fecha en la que se requiere verificar el performance del modelo. Está en formato yyyymmdd.
            - cant_bines: Cantidad de bines que se creará con la distribución del score. Aplica solo para el baseline. Por defecto 20.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.
            - tipo: Indica si la fecha en la cual ejecutamos la función corresponde al periodo de BASELINE o algun otro periodo (performance). Por defecto es 'PERFORMANCE'. 
                    Si es BASELINE crea la tabla de performance en base a la distribución del score y genera el baseline de PSI, si es PERFORMANCE usa los puntos del corte del último baseline.
                    
        Outputs:
        --------
        - df_res_bines, df_res_metricas, df_psi: Dataframes en spark que contienen la tabla de performance, la tabla con las métricas de performance y el baseline del psi (si se ejecuta el BASELINE, caso contrario devuelve un dataframe vacio),respectivamente.

## insert_performance
Función que inserta en hadoop la tabla del baseline de vdi

    Inputs:
    -------
            - df_res_bines: Dataframe en spark que contienen la tabla de performance
            - df_res_metricas: Dataframe en spark que contienen la tabla con las métricas de performance
            - df_psi: Dataframe que tiene el baseline del psi (si se ejecuta el BASELINE). Por defecto 0.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.
            - tipo: Indica si el dataset generado corresponde al periodo de BASELINE o algun otro periodo (performance). Por defecto es 'PERFORMANCE'. 
            
## psi
Función que calcula el psi (Population Stability Index) para los modelos de clasificación. E inserta en la tabla de psi el calculo del contribution_to_index por bin (el psi es la suma de contribution_to_index)

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
        
## insert_psi
Función que inserta en hadoop la tabla del baseline de vdi

    Inputs:
    -------
        - df_psi: Dataframe que tiene el psi del modelo
        - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.
        
## vdi_cualitativas
Función que calcula el vdi para las variables cualitativas.
Nota: Previamente hay que definir un baseline de las variables cualitativas.

    Inputs:
    -------
        - id: Nombre del campo que identifica el 'id' del modelo por ejemplo el campo 'linea' o el campo 'id_suscripcion'.
        - score: Nombre del campo que identifica el 'score' del modelo por ejemplo el campo 'score_capro'. 
        - variables: lista de variables cualitativas a monitorear.
        - fecha_foto: Fecha en la que se requiere verificar la estabilidad de la población scoreada. Está en formato yyyymmdd.
        - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo a monitorear la estabilidad.
        - nombre_modelo: Nombre del modelo a monitorear la estabilidad.
        - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto sdb_datamining
    
    Outputs:
    -------
        - df_vdi_cuali_move: dataframe de spark con el calculo del vdi de cada una de las variables cualitativas.
        
## vdi_cuantitativas
Función que calcula el vdi para las variables cuantitativas.
Nota: Previamente hay que definir un baseline de las variables cuantitativas.

    Inputs:
    -------
        - id: Nombre del campo que identifica el 'id' del modelo por ejemplo el campo 'linea' o el campo 'id_suscripcion'.
        - score: Nombre del campo que identifica el 'score' del modelo por ejemplo el campo 'score_capro'. 
        - variables: lista de variables cuantitativas a monitorear. (solo 10 variables)
        - fecha_foto: Fecha en la que se requiere verificar la estabilidad de la población scoreada. Está en formato yyyymmdd.
        - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo a monitorear la estabilidad.
        - nombre_modelo: Nombre del modelo a monitorear la estabilidad.
        - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto sdb_datamining 
    
    Outputs:
    --------
        - df_vdi_cuanti_move: dataframe en spark con el calculo del vdi de cada una de las variables cuantitativas.
        
        
## insert_vdi
Función que inserta en hadoop la tabla del baseline de vdi (aplica para cualitativas y cuantitativas)

    Inputs:
    -------
        - df_vdi: Dataframe que tiene el el vdi de las variables cualitativas o cuantitativas del modelo.
        - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.