import pandas as pd
import numpy as np
from datetime import datetime
# Quitamos los mensajes de warning
import warnings
    
class funciones:
    def __init__(self) -> None:
        pass
        
    
    @staticmethod
    def vdi_bl(id, fecha_foto, variables, abt_modelo, nombre_modelo, tipo_variables='cualitativas', ambiente='sdb_datamining'):
        """Función que calcula el baseline de la distribución de las variables cuantitativas y cualitativas.

        Inputs:
        -------
            - id: Nombre del campo que identifica el 'id' del modelo por ejemplo el campo 'linea' o el campo 'id_suscripcion'.
            - fecha_foto: fecha en la que se requiere ver la distribución de las variables yyyymmdd.
            - variables: lista de variables a monitorear. No más de 20 variables.
            - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo.
            - nombre_modelo: Nombre del modelo a monitorear el performance.
            - tipo_variables: Indica si el grupo de variables es cualitativa o cuantitativa. Por defecto 'CUALITATIVAS'.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.

        Outputs:
        -------
            - df_vdi_bl: Dataframe con la distribución baseline de las variables

        """
        import pandas as pd
        import numpy as np
        from datetime import datetime
        from pyspark.shell import spark
        # Quitamos los mensajes de warning
        import warnings
        warnings.filterwarnings('ignore')

        # Validamos que se pasen como parámetro solamente 20 variables
        assert len(variables)<=20, 'Debe insertar 20 variables como máximo'

        # Genemos el periodo de score
        fecha_foto_dt = datetime.strptime(fecha_foto, '%Y%m%d')
        periodo = int(fecha_foto_dt.strftime('%Y%m'))

        # Creamos el query que levanta las n variables especificadas en 'variables' y lo pasamos a pandas

        query = f"""select {id}
                 """
        for i in range(0, len(variables)):
            query += f"""
            ,{variables[i]}"""
            # Guardamos en un string las variables para levantar del baseline
            if i ==0:
                lista_variables = f"""'{variables[i]}'"""
            else:
                lista_variables += f""",'{variables[i]}'"""

        query += f"""
            from {ambiente}.{abt_modelo} 
            where periodo={periodo}"""

        df_baseline = spark.sql(query)
        df_baseline=df_baseline.toPandas()

        # Generamos el baseline
        ## Definimos dataframe que acumule los baseline de las variables
        df_vdi_bl=pd.DataFrame()

        ## Cuantitativas
        if tipo_variables.upper().strip() == 'CUANTITATIVAS':

            for variable in variables:

                # Definimos 10 grupos en base a la distribución de la variable
                sub = df_baseline[[variable]]
                sub=sub.sort_values(by =[variable])
                sub['rank']=pd.qcut(sub[variable], q=10,duplicates='drop')

                # Calculamos los totales, valor_maximo y valor_minimo en cada rango
                count = sub.groupby(['rank'])[[variable]].count().rename(columns={variable:"totales"})
                max_val= sub.groupby(['rank'])[[variable]].max().rename(columns={variable:"max_val"})
                min_val= sub.groupby(['rank'])[[variable]].min().rename(columns={variable:"min_val"})

                # Joineamos cada unos de los valores
                tmp_var=pd.merge(count, max_val,  on = 'rank')
                tmp_var=pd.merge(tmp_var, min_val,  on = 'rank')

                # Añadimos el nombre de la variable por la que estamos iterando
                tmp_var['var']=variable

                # Añadimos el número de bin
                tmp_var=tmp_var.reset_index()
                tmp_var['bin']= tmp_var.index.tolist()

                # Seleccionamos los campos
                var = tmp_var[['var', 'bin', 'totales', 'min_val','max_val']]

                # Appendemos los baseline de cada variable
                df_vdi_bl=df_vdi_bl.append(var)

            # Seteamos campos complementarios necesarios para tabla
            df_vdi_bl['fecha_foto']=fecha_foto
            df_vdi_bl['modelo']= nombre_modelo
            df_vdi_bl['positivos']= 0
            df_vdi_bl['min_p']= 0
            df_vdi_bl['max_p']= 0

            df_vdi_bl=df_vdi_bl[['var', 'bin', 'totales', 'positivos','min_p', 'max_p','min_val','max_val', 'fecha_foto', 'modelo']]

            # Transformamos a spark
            df_vdi_bl =spark.createDataFrame(df_vdi_bl)

         ## Cualitativas
        if tipo_variables.upper().strip() == 'CUALITATIVAS':
            for variable in variables:

                # Calculamos los totales por cada categoría
                sub = df_baseline[[variable]]
                sub=sub.sort_values(by =[variable])
                tmp_var = sub.groupby([variable])[[variable]].count().rename(columns={variable:"totales"})

                # Añadimos el nombre de la variable por la que estamos iterando
                tmp_var['var']=variable
                tmp_var['bin']= tmp_var.index.tolist()
                tmp_var['bin'] = tmp_var['bin'].astype(str)

                # Seleccionamos los campos
                var = tmp_var[['bin','var', 'totales']]

                # Appendemos los baseline de cada variable
                df_vdi_bl=df_vdi_bl.append(var)

            # Seteamos campos complementarios necesarios para la tabla
            df_vdi_bl['fecha_foto']=fecha_foto
            df_vdi_bl['modelo']= nombre_modelo
            df_vdi_bl['categoria']=df_vdi_bl['bin']
            df_vdi_bl['bin']=0
            df_vdi_bl=df_vdi_bl[['bin','var','totales','categoria', 'fecha_foto', 'modelo']]

            # Transformamos a spark
            df_vdi_bl =spark.createDataFrame(df_vdi_bl)

        return df_vdi_bl
    
    @staticmethod
    def insert_vdi_bl(df_vdi_bl, tipo_variables='cualitativas', ambiente='sdb_datamining'):
        """Función que inseta en hadoop la tabla del baseline de vdi
        Inputs:
        -------
            - df_vdi_bl: Dataframe de spark a insertar en hadoop.
            - tipo_variables: Indica si el grupo de variables es cualitativa o cuantitativa. Por defecto 'CUALITATIVAS'.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.
        """
        # Levantamos spark
        from pyspark.shell import Spark

        # Seteamos a nonstrict el partition mode
        result = spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
        flag = False

        if tipo_variables.lower() == 'cualitativas':
            table = 'indicadores_vdi_cuali_bl'
            flag = True
        elif tipo_variables.lower() == 'cuantitativas':
            table = 'indicadores_vdi_cuanti_bl'
            flag = True

        if flag:
            df_vdi_bl.write.mode("overwrite").insertInto(f"{ambiente}.{table}",overwrite=True)   
            print(f'Va insertó {df_vdi_bl.count()} registros en la tabla {table}')
        else:
            print('El tipo de variable tiene que ser cualitativas o cuantitativas')
            
            
    @staticmethod
    def performance(abt_modelo, nombre_modelo, query, fecha_score, cant_bines = 20, ambiente='sdb_datamining', tipo= 'PERFORMANCE'):
        """
        Función que calcula las métricas de performance para los modelos de clasificación binaria, tales como Information Value, Kolmogorov Smirnov (KS), Gini, AUC. 
        También, crea la tabla de performance en donde podemos apreciar métricas tales como Lift Acumulado, % Captura, % Conversión.
        En el caso de que el parámetro 'tipo' tenga valor 'BASELINE'se creará un dataframe que tenga el psi del periodo especificado.
        Importante: Enviamos como parámetro el query que joinea el target con el score.

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

        """
        # Importamos librerías
        import pandas as pd
        import numpy as np 
        from datetime import datetime, timedelta
        from calendar import monthrange
        from dateutil.relativedelta import relativedelta
        from pyspark.shell import spark
        # Quitamos los mensajes de warning
        import warnings
        warnings.filterwarnings('ignore')

        # Levantamos el join entre el target y la abt
        df_val = spark.sql(query)
        df_bin=df_val.toPandas()

        # Si tipo = 'PERFORMANCE' entonces cargamos el baseline y sus respectivos puntos de corte.
        if tipo == 'PERFORMANCE':
            # Cargamos el último baseline y lo pasamos a pandas
            query=f"""
            select max_p_target from {ambiente}.indicadores_bines 
            where modelo='{nombre_modelo}' and tipo='BASELINE' 
            and fecha =(select max(fecha) from {ambiente}.indicadores_bines 
                    where modelo='{nombre_modelo}' and tipo='BASELINE' )
            order by max_p_target"""
            df_baseline=spark.sql(query)
            df_baseline=df_baseline.toPandas()

            # Ordenamos los puntos de corte
            df_baseline = df_baseline.sort_values(by='max_p_target')


            # Capturamos los puntos de corte
            cut_bin=df_baseline['max_p_target'].tolist()
            cut_bin.insert(0, 0)
            # Asignamos el 1 como la máxima probabilidad posible
            cut_bin[-1] = 1

            # Definimos percentiles en base a los puntos de corte del baseline
            df_bin['percentil']=pd.cut(df_bin['score'] , bins=cut_bin)

         # Si tipo = 'BASELINE' entonces calculamos la tabla de performance y la tabla de métricas de performance considerando la distribución del score.
        elif tipo == 'BASELINE':
            # Definimos percentiles en base a la distribución del score
            df_bin['percentil']=pd.qcut(df_bin['score'], cant_bines, duplicates='drop')

        # Ordenemos la tabla y definimos los bines
        df_bin=df_bin.sort_values(by=['percentil'])
        bines= pd.DataFrame({'percentil':df_bin.percentil.unique()})
        bines['Bin'] = bines.index
        df_bin=pd.merge(df_bin, bines, how='left', on=['percentil', 'percentil'])

        # Calculamos positivos y totales por grupo
        df_res = df_bin.groupby('percentil').agg({'target':'sum','score':'count'}).reset_index()

        # Calculamos la tasa por grupo
        df_res['porcentaje'] =100* df_res['target']/df_res['score']

        # Capturamos la probabilidad máxima y mínima por grupo
        a=df_res['percentil'].astype(str)
        df_res['max_p_target']=a.str.slice(start=-7).str.replace(']','').str.replace('1,','').str.replace('2,','').str.replace('3,','').str.replace('4,','').str.replace('5,','').str.replace('6,','').str.replace('7,','').str.replace('8,','').str.replace('9,','').str.replace(', ','').astype(float)
        df_res['min_p_target']=a.str.slice(1,6).str.replace(',','').astype(float)

        # Renombramos los campos calculados/generados
        df_res=df_res.rename(columns={'target':'Positivos',
                                      'score':'Totales',
                                      'percentil':'Bin'
                                      })

        # Calculamos positivos y totales acumulados
        df_res['positivos_acum']= np.cumsum(df_res['Positivos'][::-1])[::-1] 
        df_res['totales_acum']= np.cumsum(df_res['Totales'][::-1])[::-1] 

        # Calculamos la tasa
        Positivos_Totales=df_res['Positivos'].sum()
        Totales=df_res['Totales'].sum()
        Tasa= df_res['Positivos'].sum() / df_res['Totales'].sum()

        # Calculamos lift, porcentaje de captura y porcentaje de conversión
        df_res['lift_acum']=(df_res['positivos_acum'] / df_res['totales_acum']) / Tasa
        df_res['porc_captura']=df_res['positivos_acum'] / Positivos_Totales
        df_res['porc_conversion']=df_res['positivos_acum']/df_res['totales_acum']

        # Seteamos campos complementarios necesarios para el dashboard
        df_res['fecha']=fecha_score
        df_res['anio'] = pd.DatetimeIndex(df_res['fecha']).year
        df_res['mes'] = pd.DatetimeIndex(df_res['fecha']).month
        df_res['dia'] = pd.DatetimeIndex(df_res['fecha']).day
        df_res['modelo'] = nombre_modelo
        df_res['tipo'] = tipo

        df_res['bin']=df_res['Bin'].index.tolist()
        Bin = df_res.pop('Bin')

        # Ordenamos los campos
        df_res_move=df_res[['anio', 'mes', 'dia', 'bin', 'Positivos', 'Totales', 'min_p_target', 'max_p_target'
                            , 'positivos_acum', 'totales_acum', 'lift_acum',  'porc_captura', 'porc_conversion','fecha','tipo','modelo' ]]

        # Transformamos a spark
        df_res_bines =spark.createDataFrame(df_res_move)

        # Si tipo es BASELINE insertamos el baseline del PSI, caso contrario, creamos un dataframe de psi vacio.
        if tipo == 'BASELINE':
            df_psi = df_res[['bin', 'max_p_target', 'Totales', 'fecha','tipo','modelo' ]]
            df_psi['tipo'] = cant_bines # en el caso del baseline del psi el tipo hace referencia a la cantidad de bines que usa 
            df_psi = spark.createDataFrame(df_psi)
            #df_psi.write.mode("overwrite").insertInto(f"{ambiente}.indicadores_psi_bl",overwrite=True)
        elif tipo == 'PERFORMANCE':
            df_psi = pd.DataFrame()

        # Indicadores de performance

        # Nos quedamos con el bin, la maxima probabilidad, totales y positivos por bin
        df_per=df_res[['bin', 'max_p_target','Totales', 'Positivos']]
        df_per['total_porc']=df_per['Totales']/Totales

        # Renombramos campos
        df_per=df_per.rename(columns={'max_p_target':'Score',
                                    'Totales':'total_nro_accts',
                                    'Positivos':'total_nro_goods'
                                })
        # Calculamos totales
        df_per['total_nro_bads'] = df_per['total_nro_accts']-df_per['total_nro_goods']
        Total_Total_nro_goods = df_per['total_nro_goods'].sum()
        Total_Total_nro_bads = df_per['total_nro_bads'].sum()
        Total_Total_nro_accts = df_per['total_nro_accts'].sum()

        # Calculamos métricas para determinar el information value, el KS y la curva ROC
        df_per['good_porc']= df_per['total_nro_goods'] / Total_Total_nro_goods
        df_per['cum_nro_goods']= np.cumsum(df_per['total_nro_goods'])
        df_per['cum_porc_goods']= df_per['cum_nro_goods'] / Total_Total_nro_goods
        df_per['bad_porc'] = df_per['total_nro_bads'] / Total_Total_nro_bads
        df_per['cum_nro_bads'] = np.cumsum(df_per['total_nro_bads'])
        df_per['cum_porc_bads'] = df_per['cum_nro_bads'] / Total_Total_nro_bads
        df_per['intvl_bad_porc'] = df_per['total_nro_bads']/df_per['total_nro_accts']

        df_per['cumulative_bad_rate_deno']=Total_Total_nro_accts - np.cumsum(df_per['total_nro_accts']).shift().fillna(0)
        df_per['cumulative_bad_rate_num']=Total_Total_nro_bads-np.cumsum(df_per['total_nro_bads']).shift().fillna(0)

        df_per['cumulative_bad_rate'] = df_per['cumulative_bad_rate_num'] / df_per['cumulative_bad_rate_deno']
        df_per['spread_porc'] = df_per['cum_porc_bads']-df_per['cum_porc_goods'].abs()
        df_per['actual_odds'] = df_per['total_nro_goods'] / df_per['total_nro_bads']

        # Calculamos el information value y la curva roc en cada uno de los bines
        df_per['odds_cumulative'] = np.cumsum(df_per['total_nro_bads'][::-1])[::-1] / np.cumsum(df_per['total_nro_goods'][::-1])[::-1] 
        df_per['informatiovalue']=(df_per['bad_porc'] - df_per['good_porc'])  * (np.log(df_per['bad_porc']/df_per['good_porc']))
        df_per['pg'] = df_per['cum_porc_bads']
        df_per['pb'] = df_per['cum_porc_goods']

        df_per['pgi'] = df_per['pg'] - df_per['pg'].shift().fillna(0)
        df_per['pbi'] = df_per['pb'] - df_per['pb'].shift().fillna(0)
        df_per['area_rectangulo'] = df_per['pbi'] * df_per['pg'].shift().fillna(0)
        df_per['area_triangulo'] = df_per['pgi'] * df_per['pbi'] / 2
        df_per['suma_area'] = df_per['area_rectangulo'] + df_per['area_triangulo']

        # Seteamos campos complementarios necesarios para el dashboard
        df_per['fecha']=fecha_score
        df_per['anio'] = pd.DatetimeIndex(df_per['fecha']).year
        df_per['mes'] = pd.DatetimeIndex(df_per['fecha']).month
        df_per['dia'] = pd.DatetimeIndex(df_per['fecha']).day
        df_per['modelo'] = nombre_modelo
        df_per['tipo'] = tipo

        # Ordenamos los campos
        df_per=df_per[['anio','mes', 'dia','bin',	'total_nro_accts',	'total_porc',	'total_nro_goods',	'good_porc',	'cum_nro_goods',	'cum_porc_goods',	'total_nro_bads',	'bad_porc',	'cum_nro_bads',	'cum_porc_bads',	'intvl_bad_porc',	'cumulative_bad_rate',	'spread_porc',	'actual_odds',	'odds_cumulative',	'informatiovalue',	'pg',	'pb',	'area_rectangulo',	'area_triangulo',	'suma_area',	'fecha','tipo',	'modelo' ]]

        # Transformamos a spark
        df_res_metricas =spark.createDataFrame(df_per)


        return df_res_bines, df_res_metricas, df_psi
    
    @staticmethod
    def insert_performance(df_res_bines, df_res_metricas, df_psi=0, ambiente='sdb_datamining', tipo = 'PERFORMANCE'):
        """Función que inserta en hadoop la tabla del baseline de vdi
        Inputs:
        -------
            - df_res_bines: Dataframe en spark que contienen la tabla de performance
            - df_res_metricas: Dataframe en spark que contienen la tabla con las métricas de performance
            - df_psi: Dataframe que tiene el baseline del psi (si se ejecuta el BASELINE). Por defecto 0.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.
            - tipo: Indica si el dataset generado corresponde al periodo de BASELINE o algun otro periodo (performance). Por defecto es 'PERFORMANCE'. 
        """
        
        # Seteamos a nonstrict el partition mode
        result = sqlContext.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
        
        # Insertamos la tabla de performance 
        df_res_bines.write.mode("overwrite").insertInto(f"{ambiente}.indicadores_bines",overwrite=True)
        
        # Insertamos la tabla de métricas de performance
        df_res_metricas.write.mode("overwrite").insertInto(f"{ambiente}.indicadores_performance",overwrite=True)
        
        # Insertarmos la tabla de baseline de PSI solamente si se especifica que estamos trabajando con el periodo de BASELINE
        if tipo.lower() == 'baseline':
            df_psi.write.mode("overwrite").insertInto(f"{ambiente}.indicadores_psi_bl",overwrite=True)
            print(f'Insertamos {df_res_bines.count()} registros en la tabla indicadores_bines, {df_res_metricas.count()} en la tabla indicadores_performance y {df_psi.count()} en la tabla indicadores_psi_bl')
        else:
            print(f'Insertamos {df_res_bines.count()} registros en la tabla indicadores_bines, {df_res_metricas.count()} en la tabla indicadores_performance.')
            
   

    @staticmethod
    def psi(id, score, fecha_foto, abt_modelo, nombre_modelo, ambiente = 'sdb_datamining'):
        """
        Función que calcula el psi (Population Stability Index) para los modelos de clasificación.
        E inserta en la tabla de psi el calculo del contribution_to_index por bin (el psi es la suma de contribution_to_index)

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

        """
        # Importamos librerías
        import pandas as pd
        import numpy as np
        from datetime import date, datetime
        from pyspark.shell import spark

        # Calculamos el periodo en base a la fecha de la foto de los datos
        fecha_foto_dt = datetime.strptime(fecha_foto, '%Y%m%d')
        periodo = int(fecha_foto_dt.strftime('%Y%m'))

        # Cargamos el periodo actual y lo pasamos a pandas
        query =f"""
        select {id} as id ,{score} as score from {ambiente}.{abt_modelo} where periodo={periodo}
        """
        df_bin_ = spark.sql(query)
        df_bin = df_bin_.toPandas()

        # Cargamos el último baseline y lo pasamos a pandas
        query=f"""
        select fecha, bin, max_p_target, totales as baseline_counts 
        from {ambiente}.indicadores_psi_bl
        where modelo='{nombre_modelo}'
        and fecha = (select max(fecha) from {ambiente}.indicadores_psi_bl where  modelo='{nombre_modelo}') 
        order by bin
        """
        df_baseline_psi_= spark.sql(query)
        df_baseline_psi = df_baseline_psi_.toPandas()
        # Ordenamos los puntos de corte
        df_baseline_psi = df_baseline_psi.sort_values(by='max_p_target')

        # Capturamos los puntos de corte
        cut_bin=df_baseline_psi['max_p_target'].tolist()
        cut_bin.insert(0, 0)
        # Asignamos el 1 como la máxima probabilidad posible
        cut_bin[-1] = 1

        # Joineamos baseline con el periodo actual. Calculo del PSI

        ## Definimos percentiles en base a los puntos de corte del baseline
        df_bin['percentil']=pd.cut(df_bin['score'] , bins=cut_bin, duplicates='drop')

        ## Ordenemos la tabla y definimos los bines
        df_bin=df_bin.sort_values(by=['percentil'])
        bines= pd.DataFrame({'percentil':df_bin.percentil.unique()})
        bines['Bin'] = bines.index

        ## Añadimos el campo de bines
        df_bin=pd.merge(df_bin, bines, how='left', on=['percentil', 'percentil'])
        df_bin=df_bin[['id', 'score', 'percentil', 'Bin']]

        ## Calculamos totales y el maximo score por bin
        df_res = df_bin.groupby('Bin').agg({'id':'count','score': 'max'}).reset_index()
        df_res.rename(columns = {'id':'validation_counts','score': 'max_score'}, inplace = True)

        ## Nos quedamos con las columnas necesarias para el baseline
        df_baseline_psi=df_baseline_psi[['max_p_target', 'baseline_counts']]

        ## Joineamos el baseline y periodo actual
        df_ind_PSI = pd.concat([df_baseline_psi, df_res], axis = 1, join="inner")

        # Calculamos el PSI

        baseline_counts_total= df_ind_PSI['baseline_counts'].sum()
        validation_counts_total = df_ind_PSI['validation_counts'].sum()

        # Reemplazamos el 0 por el 1 en el 'validation_counts' para poder calcular el 'ratio' y el resto de métricas
        df_ind_PSI['validation_counts'] = np.where(df_ind_PSI['validation_counts']== 0,1,df_ind_PSI['validation_counts'])

        df_ind_PSI['baseline_porc'] = df_ind_PSI['baseline_counts'] / baseline_counts_total
        df_ind_PSI['validation_porc'] = df_ind_PSI['validation_counts'] / validation_counts_total
        df_ind_PSI['difference'] = df_ind_PSI['baseline_porc'] - df_ind_PSI['validation_porc']
        df_ind_PSI['ratio'] = df_ind_PSI['baseline_porc'] / df_ind_PSI['validation_porc']
        df_ind_PSI['weight_of_evidence'] = np.log(df_ind_PSI['ratio']) 
        df_ind_PSI['contribution_to_index'] = df_ind_PSI['difference'] * df_ind_PSI['weight_of_evidence']

        # Añadimos campos para el dashboard de monitoreo
        # Seteamos campos complementarios necesarios para el dashboard
        df_ind_PSI['fecha'] = fecha_foto
        df_ind_PSI['anio'] = pd.DatetimeIndex(df_ind_PSI['fecha']).year
        df_ind_PSI['mes'] = pd.DatetimeIndex(df_ind_PSI['fecha']).month
        df_ind_PSI['dia'] = pd.DatetimeIndex(df_ind_PSI['fecha']).day
        df_ind_PSI['modelo'] = nombre_modelo

        # Ordenamos los campos
        df_ind_PSI=df_ind_PSI[['anio','mes','dia', 'Bin',
                                 'baseline_counts', 'baseline_porc', 'validation_counts', 'validation_porc',
                                 'difference', 'ratio', 'weight_of_evidence', 'contribution_to_index','fecha', 'modelo']]

        # Transformamos a spark
        df_ind_PSI_move =spark.createDataFrame(df_ind_PSI)


        return df_ind_PSI_move

    @staticmethod
    def insert_psi(df_psi, ambiente='sdb_datamining'):
        """Función que inserta en hadoop la tabla del baseline de vdi
        Inputs:
        -------
            - df_psi: Dataframe que tiene el psi del modelo
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.
        """
        # Levantamos la sesión de spark
        from pyspark.sql import SparkSession, SQLContext

        spark = SparkSession.builder.appName("Monitoring Functions").getOrCreate()
        sc=spark.sparkContext
        sqlContext = SQLContext(sc)

        # Seteamos a nonstrict el partition mode
        result = sqlContext.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")

        # Insertamos la tabla de psi 
        df_psi.write.mode("overwrite").insertInto(f"{ambiente}.indicadores_psi",overwrite=True)

        print(f'Insertamos {df_psi.count()} registros en la tabla indicadores_psi.')
        
        
    @staticmethod
    def vdi_cualitativas(id, score, variables,fecha_foto, abt_modelo, nombre_modelo, ambiente='sdb_datamining' ):
        """
        Función que calcula el vdi para las variables cualitativas.
        Nota: Previamente hay que definir un baseline de las variables cualitativas.

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

        """
        # Importamos librerias 
        import pandas as pd
        import numpy as np
        from datetime import datetime
        from pyspark.shell import spark
        # Quitamos los mensajes de warning
        import warnings
        warnings.filterwarnings('ignore')

        # Validamos que se ingrese la cantidad de variables adecuada
        assert len(variables)<=20 , "No puede insertar más de 20 variables. Las variables deben estar definidas en la tabla de baseline"

        # Calculamos el periodo en base a la fecha de la foto de los datos
        fecha_foto_dt = datetime.strptime(fecha_foto, '%Y%m%d')
        periodo = int(fecha_foto_dt.strftime('%Y%m'))

        # Creamos el query que levanta las n variables especificadas en 'variables' y lo pasamos a pandas

        query = f"""select {id} as id
                 """
        for i in range(0, len(variables)):
            query += f"""
            ,cast({variables[i]} as string) as {variables[i]}"""
            # Guardamos en un string las variables para levantar del baseline
            if i ==0:
                lista_variables = f"""'{variables[i]}'"""
            else:
                lista_variables += f""",'{variables[i]}'"""
        query += f"""
            ,{score} as score
            from {ambiente}.{abt_modelo} 
            where periodo={periodo}"""

        df_new_ = spark.sql(query)
        df_new=df_new_.toPandas()

        # Levantamos el último baseline de las variables cualitativas y la pasamos a pandas
        query=f"""
        select var, categoria, totales
        from {ambiente}.indicadores_vdi_cuali_bl
        where modelo ='{nombre_modelo}' 
        and fecha= (select max(fecha) from {ambiente}.indicadores_vdi_cuali_bl where modelo ='{nombre_modelo}' )
        and var in ({lista_variables})
        """
        df_vdi_cuali_bl= spark.sql(query)
        df_vdi_cuali_bl=df_vdi_cuali_bl.toPandas()

        # Generamos grupos en base a las categorías
        ## Definimos dataframes temporales
        df_vdi_cuali_new = pd.DataFrame()
        tmp_var = pd.DataFrame()
        tmp_var_new = pd.DataFrame()

        ## Iteramos por variable
        for variable in variables:

            sub = df_new[['score',variable]]
            count = sub.groupby([variable]).count().rename(columns={variable:"totales"})

            ### Calculamos los totales por categoría
            tmp_var_new=count.reset_index()
            tmp_var_new=tmp_var_new.rename(columns={variable:"categoria", "score":"totales"})
            tmp_var_new['var']=variable
            var = tmp_var_new[['categoria','var', 'totales']]

            ### Appedeamos los resultados
            df_vdi_cuali_new=df_vdi_cuali_new.append(var)

        # Joineamos Baseline con periodo actual. Luego, calculamos el vdi de cada una de las variables
        ## Pasamos a minúsculas las categorías tanto en el periodo actual como en el baseline
        df_vdi_cuali_bl['categoria']=df_vdi_cuali_bl['categoria'].str.lower()
        df_vdi_cuali_new['categoria']=df_vdi_cuali_new['categoria'].str.lower()

        ## Joineamos las variables a monitorear con su baseline (df inicial) con un inner join 
        df_ind_VDI = pd.merge( df_vdi_cuali_bl, df_vdi_cuali_new, on=["var", "categoria"])

        ## Colocamos los nombres que corresponden
        df_ind_VDI= df_ind_VDI.rename(columns = {
                                    'totales_x':'baseline_counts',
                                    'totales_y':'validation_counts'})
        df_ind_VDI=df_ind_VDI.fillna(0)

        ## Creamos dataframes temporales
        df_temp = pd.DataFrame()
        df_vdi_cuali2 = pd.DataFrame()

        ### Por cada variable calculamos el VDI hacemos un append y guardamos los resultados en la tabla df_vdi_cuali2
        for variable in variables:

            df_temp=df_ind_VDI[ (df_ind_VDI['var']==variable) ]

            baseline_counts_total= df_temp['baseline_counts'].sum()
            validation_counts_total = df_temp['validation_counts'].sum()

            # Reemplazamos el 0 por el 1 en el 'validation_counts' para poder calcular el 'ratio' y el resto de métricas
            df_temp['validation_counts'] = np.where(df_temp['validation_counts']== 0,1,df_temp['validation_counts'])

            df_temp['baseline_porc'] = df_temp['baseline_counts'] / baseline_counts_total
            df_temp['validation_porc'] = df_temp['validation_counts'] / validation_counts_total
            df_temp['difference'] = df_temp['baseline_porc'] - df_temp['validation_porc']
            df_temp['ratio'] = df_temp['baseline_porc'] / df_temp['validation_porc']
            df_temp['weight_of_evidence'] = np.log(df_temp['ratio']) 
            df_temp['contribution_to_index'] = df_temp['difference'] * df_temp['weight_of_evidence']
            var = df_temp[['categoria','var', 'baseline_counts', 'baseline_porc', 'validation_counts', 'validation_porc', 'difference', 'ratio', 'weight_of_evidence', 'contribution_to_index']]

            df_vdi_cuali2=df_vdi_cuali2.append(var)

        # Añadimos campos para el dashboard de monitoreo
        ## Seteamos campos complementarios necesarios para el dashboard
        df_vdi_cuali2['fecha'] = fecha_foto
        df_vdi_cuali2['anio'] = pd.DatetimeIndex(df_vdi_cuali2['fecha']).year
        df_vdi_cuali2['mes'] = pd.DatetimeIndex(df_vdi_cuali2['fecha']).month
        df_vdi_cuali2['dia'] = pd.DatetimeIndex(df_vdi_cuali2['fecha']).day
        df_vdi_cuali2['bin']=''
        df_vdi_cuali2['val_max']=''
        df_vdi_cuali2['modelo'] = nombre_modelo
        df_vdi_cuali2['tipo_variable'] = 'cualitativa'

        ## Ordenamos los campos
        df_vdi_cualii=df_vdi_cuali2[['anio','mes','dia','bin','val_max','var',
                                 'baseline_counts', 'baseline_porc', 'validation_counts', 'validation_porc',
                                 'difference', 'ratio', 'weight_of_evidence', 'contribution_to_index','categoria','fecha','modelo','tipo_variable']]

        ## Transformamos a spark e insertamos
        df_vdi_cuali_move =spark.createDataFrame(df_vdi_cualii)

        return df_vdi_cuali_move
    
    @staticmethod
    def vdi_cuantitativas(id, score, variables,fecha_foto, abt_modelo, nombre_modelo, ambiente='sdb_datamining'):
        """
        Función que calcula el vdi para las variables cuantitativas.
        Nota: Previamente hay que definir un baseline de las variables cuantitativas.

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
        """

        # Importamos librerias 
        import pandas as pd
        import numpy as np
        from datetime import datetime
        from pyspark.shell import spark
        # Quitamos los mensajes de warning
        import warnings
        warnings.filterwarnings('ignore')

        # Validamos que se pasen como parámetro máximo 20 variables
        assert len(variables)<=20 , "No puede insertar más de 20 variables. Las variables deben estar definidas en la tabla de baseline"

        # Calculamos el periodo en base a la fecha de la foto de los datos
        fecha_foto_dt = datetime.strptime(fecha_foto, '%Y%m%d')
        periodo = int(fecha_foto_dt.strftime('%Y%m'))

        # Creamos el query que levanta las n variables especificadas en 'variables' y lo pasamos a pandas

        query = f"""select {id} as id
                 """
        for i in range(0, len(variables)):
            query += f"""
            ,{variables[i]}"""
            # Guardamos en un string las variables para levantar del baseline
            if i ==0:
                lista_variables = f"""'{variables[i]}'"""
            else:
                lista_variables += f""",'{variables[i]}'"""

        query += f"""
            ,{score} as score
            from {ambiente}.{abt_modelo} 
            where periodo={periodo}"""

        df_actual_ = spark.sql(query)
        df_actual=df_actual_.toPandas()

        # Levantamos el último baseline de las variables cualitativas
        query=f"""
        select * from {ambiente}.indicadores_vdi_cuanti_bl 
        where modelo = '{nombre_modelo}'
        and fecha = (select max(fecha) from {ambiente}.indicadores_vdi_cuanti_bl where  modelo='{nombre_modelo}') 
        and var in ({lista_variables})
        """
        df_vdi_cuanti_bl= spark.sql(query)

        ## Pasamos el dataframe a pandas
        df_vdi_cuanti_bl_=df_vdi_cuanti_bl.toPandas()

        ## Nos quedamos con las columnas var, bin y max_val
        df_vdi_cuanti_bl = df_vdi_cuanti_bl_[['var', 'bin', 'min_val', 'max_val']]

        ## Seteamos el indice del dataframe por var (variable)
        df_vdi_cuanti_bl.set_index('var', inplace=True)

        # Generamos grupos en base a las categorías
        ## Definimos un dataframe temporal
        df_vdi_cuanti = pd.DataFrame()

        #Para todas las variables en variables calculamos la distribución por bin y los valores máximos en base al baseline definido
        for variable  in variables:
            # Tomamos el max_val y el bin de la variable
            df_baseline_vdi = df_vdi_cuanti_bl.loc[variable][['min_val','max_val', 'bin']].sort_values(by='bin')

            # Tomamos la columnas max_val y la pasamos a lista
            cut_val=df_baseline_vdi['max_val'].tolist()

            # Definimos el mínimo valor por variable
            min_val = df_baseline_vdi['min_val'].tolist()[0]-10000 #para incluir el minimo valor

            # Añadimos un valor mínimo al inicio de la lista
            cut_val.insert(0,min_val)

            # Actualizamos el último punto de corte para capturar todos los casos
            cut_val[-1] *=  100

            # Ordenamos los puntos de corte de menor a mayor
            cut_val = np.sort(cut_val)

            # Nos quedamos con la variable a monitorear
            df_val = pd.DataFrame(df_actual[variable])

            # Aplicamos los puntos de corte en base al baseline de esa variable
            df_val['rango'] = pd.cut(df_actual[variable] , bins=cut_val, duplicates='drop')

            # Ordenamos la variable por los valores los rangos del baseline
            df_val=df_val.sort_values(by=['rango'])

            # Calculamos los totales por grupo
            count_bin = df_val.groupby(['rango']).count().rename(columns={variable:"totales"})

            # Nos quedamos con los valores máximos de cada grupo
            max_val = df_val.groupby(['rango']).max().rename(columns={variable:"max_val"})

            # Unificamos los dos dataframes
            count=pd.concat([count_bin,max_val], axis=1,join="inner")

            # Añadimos la columna de la variable
            count['var']= variable

            # Añadimos el bin
            count = count.reset_index()
            count['bin']= count.index

            # Ordenamos los campos
            var = count[['var','bin','max_val', 'totales']]

            # Appendemos las variables
            df_vdi_cuanti=df_vdi_cuanti.append(var)

        # Casteamos a float para asegurar que tenemos el tipo de dato correcto
        df_vdi_cuanti['max_val']=df_vdi_cuanti['max_val'].astype(float)

        # Joineamos Baseline con periodo actual. Luego, calculamos el vdi de cada una de las variables
        df_vdi_cuanti_bl_.pop('max_val') # saco el valor máximo del baseline

        ## Joineamos las variables a monitorear con su baseline (df inicial) con un inner join 
        df_ind_VDI = pd.merge(df_vdi_cuanti_bl_, df_vdi_cuanti, on=["var", "bin"])

        ## Colocamos los nombres que corresponden
        df_ind_VDI= df_ind_VDI.rename(columns = {
                                'totales_x':'baseline_counts',
                                'totales_y':'validation_counts'})

        df_ind_VDI=df_ind_VDI.fillna(0) # No debería ser necesario

        # Creamos dataframes temporales
        df_temp = pd.DataFrame()
        df_vdi_cuanti2 = pd.DataFrame()

        # Rename del nombre de la columna como va en la tabla
        df_ind_VDI.rename( columns={'max_val':'val_max'} , inplace=True) 

        # Por cada variable calculamos el VDI hacemos un append y guardamos los resultados en la tabla df_vdi_cuanti2
        for variable in variables:

            df_temp=df_ind_VDI[ (df_ind_VDI['var']==variable) ]

            baseline_counts_total= df_temp['baseline_counts'].sum()
            validation_counts_total = df_temp['validation_counts'].sum()

            # Reemplazamos el 0 por el 1 en el 'validation_counts' para poder calcular el 'ratio' y el resto de métricas
            df_temp['validation_counts'] = np.where(df_temp['validation_counts']== 0,1,df_temp['validation_counts'])

            df_temp['baseline_porc'] = df_temp['baseline_counts'] / baseline_counts_total
            df_temp['validation_porc'] = df_temp['validation_counts'] / validation_counts_total
            df_temp['difference'] = df_temp['baseline_porc'] - df_temp['validation_porc']
            df_temp['ratio'] = df_temp['baseline_porc'] / df_temp['validation_porc']
            df_temp['weight_of_evidence'] = np.log(df_temp['ratio']) 
            df_temp['contribution_to_index'] = df_temp['difference'] * df_temp['weight_of_evidence']
            var = df_temp[['bin','val_max','var', 'baseline_counts', 'baseline_porc', 'validation_counts', 'validation_porc', 'difference', 'ratio', 'weight_of_evidence', 'contribution_to_index']]

            df_vdi_cuanti2=df_vdi_cuanti2.append(var)

        # Añadimos campos para el dashboard de monitoreo
        ## Seteamos campos complementarios necesarios para el dashboard
        df_vdi_cuanti2['fecha'] = fecha_foto
        df_vdi_cuanti2['anio'] = pd.DatetimeIndex(df_vdi_cuanti2['fecha']).year
        df_vdi_cuanti2['mes'] = pd.DatetimeIndex(df_vdi_cuanti2['fecha']).month
        df_vdi_cuanti2['dia'] = pd.DatetimeIndex(df_vdi_cuanti2['fecha']).day
        df_vdi_cuanti2['categoria'] = ''
        df_vdi_cuanti2['tipo_variable'] = 'cuantitativa'
        df_vdi_cuanti2['modelo'] = nombre_modelo

        ## Ordenamos los campos
        df_vdi_cuantii=df_vdi_cuanti2[['anio','mes','dia','bin','val_max','var',
                             'baseline_counts', 'baseline_porc', 'validation_counts', 'validation_porc',
                             'difference', 'ratio', 'weight_of_evidence', 'contribution_to_index','categoria','fecha','modelo','tipo_variable']]

        ## Transformamos a spark e insertamos
        df_vdi_cuanti_move =spark.createDataFrame(df_vdi_cuantii)

        return df_vdi_cuanti_move
    
    @staticmethod
    def insert_vdi(df_vdi, ambiente='sdb_datamining'):
        """Función que inserta en hadoop la tabla del vdi (aplica para cualitativas y cuantitativas)
        Inputs:
        -------
            - df_vdi: Dataframe que tiene el el vdi de las variables cualitativas o cuantitativas del modelo.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producción). Por defecto 'sdb_datamining'.
        """
        # Levantamos la sesión de spark
        from pyspark.sql import SparkSession, SQLContext

        spark = SparkSession.builder.appName("Monitoring Functions").getOrCreate()
        sc=spark.sparkContext
        sqlContext = SQLContext(sc)

        # Seteamos a nonstrict el partition mode
        result = sqlContext.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")

        # Insertamos la tabla de psi 
        df_vdi.write.mode("overwrite").insertInto(f"{ambiente}.indicadores_vdi",overwrite=True)


        print(f'Insertamos {df_vdi.count()} registros en la tabla indicadores_vdi.')