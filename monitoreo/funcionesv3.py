import pandas as pd
import numpy as np 
from datetime import datetime, timedelta #Todo: No se usa timedelta
from calendar import monthrange
from dateutil.relativedelta import relativedelta#Todo: No se usa relativedelta
from pyspark.shell import spark
        

import warnings

class Funciones:
    """Refactor de funciones de monitoreo, ahora con 30% + OOP para el deleite de la dama y el disfrute del caballero

    Metodos:
    
    def __str__(self) -> str:       -> De puro inchabolas que soy nomas
    
    def __repr__(self) -> str:      -> Tambien, pero lo uso para intentar rastrear instancias..

    Ideota: pasar vdi_bl, performance, insert_performance a funciones privadas y ejecutarlas via 
                performance_baseline, vdi_basline, o vdi y performance segun el caso... ver idea en version 3
    
    
    
    Propiedades    
        self.id=id,
        self.abt_modelo=abt_modelo,
        self.nombre_modelo=nombre_modelo,
        self.ambiente=ambiente
        self.fecha_foto=None -> reasignable con metodo set
        """
    instancias=[]
    
    
    def __init__(self, id, abt_modelo, nombre_modelo, score=None, fecha_foto=None, ambiente='sdb_datamining') -> None:
        
        #Asserts?....
              
        self.id=id,
        self.abt_modelo=abt_modelo,
        self.nombre_modelo=nombre_modelo,
        self.ambiente='sdb_datamining'
        #TODO: lista de df... desarrollar
        self.lista_df=[]
        self.score=score
        
        #Raestro de instancias... ver si funciona en zeppelin
        Funciones.instancias.append(self)

    
    def __repr__(self) -> str:
        return f"Funciones({self.id},{self.abt_modelo},{self.nombre_modelo},{self.ambiente})"
    
    def set_fecha_foto(self, fechafoto:str)->None:
        self.set_fecha_foto=fechafoto
        
    def __str__(self) -> str:
        print("id", self.id)
        print("abt_modelo", self.abt_modelo)
        print("nombre_modelo", self.nombre_modelo)
        print("ambiente", self.ambiente)

    def __sparkContext(self):
        # TODO: Revisar si todo esto es necesario
        # Levantamos la sesi??n de spark
    
        spark = SparkSession.builder.appName("Monitoring Functions").getOrCreate()
        sc=spark.sparkContext
        sqlContext = SQLContext(sc)


        # Seteamos a nonstrict el partition mode
        return sqlContext.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")    


    def __genPeriodo(self, fecha_foto):
        # Genemos el periodo de score
        fecha_foto_dt = datetime.strptime(fecha_foto, '%Y%m%d')
        return int(fecha_foto_dt.strftime('%Y%m'))
    
        
    def __insert(self, df, ambiente, tabla, silent=False):
        df.write.mode("overwrite").insertInto(f"{ambiente}.{tabla}",overwrite=True)
        print(f'Insertamos {df.count()} registros en la tabla {tabla}') if silent == False else None
        
    # def get_datasets(self):
    #     return self.lista_df
    
    # def get_df(self, df:int):
    #     return self.lista_df[df]
        
    # def show_df(self, df:int):
    #     print(self.lista_df[df])
    
       
    # TODO: Combos tipo...
    # def preformanceAndInsert(self, query, fecha_score, cant_bines = 20):
    #     self.insert_performance(self.performance(query, fecha_score, cant_bines, tipo='PERFORMANCE'), tipo='PERFORMANCE')
        
    # def baselineAndInsert(self, query, fecha_score, cant_bines = 20):
    #     self.insert_baseline(self.baseline(query, fecha_score, cant_bines, tipo='BASELINE'), tipo='BASELINE')


    # def __check_fecha_foto(fecha_foto, self)->str:
    #     if fecha_foto==None and self.fecha_foto==None:
    #         print("fecha_foto no esta definida ni pasada por parametro")
    #         raise
    #     else:
    #         return fecha_foto if fecha_foto != None else self.fecha_foto
        
    
    
    def calcular_vdi_cuantitativas_baseline(self, fecha_foto, variables, ambiente='sdb_datamining'):
        """Funci??n que calcula el baseline de la distribuci??n de las variables cuantitativas.
	
        Inputs:
        -------
            - id: Nombre del campo que identifica el 'id' del modelo por ejemplo el campo 'linea' o el campo 'id_suscripcion'.
            - fecha_foto: fecha en la que se requiere ver la distribuci??n de las variables yyyymmdd.
            - variables: lista de variables a monitorear. No m??s de 20 variables.
            - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo.
            - nombre_modelo: Nombre del modelo a monitorear el performance.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producci??n). 
                        Por defecto 'sdb_datamining'.
        Outputs:
        -------
            - df_vdi_cuanti_bl: Dataframe con la distribuci??n baseline de las variables
        """
        # Importamos librer??as

        warnings.filterwarnings('ignore')
	
        # Validamos que se pasen como par??metro solamente 20 variables
        assert len(variables)<=20, 'Debe insertar 20 variables como m??ximo'
	
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
            from {ambiente}.{self.abt_modelo} 
            where periodo={periodo}"""
	
        df_baseline = spark.sql(query)
        df_baseline=df_baseline.toPandas()
	
        # Generamos el baseline
        ## Definimos dataframe que acumule los baseline de las variables
        df_vdi_cuanti_bl=pd.DataFrame()
	
        for variable in variables:
	
            # Definimos 10 grupos en base a la distribuci??n de la variable
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
	
            # A??adimos el nombre de la variable por la que estamos iterando
            tmp_var['var']=variable
	
            # A??adimos el n??mero de bin
            tmp_var=tmp_var.reset_index()
            tmp_var['bin']= tmp_var.index.tolist()
	
            # Seleccionamos los campos
            var = tmp_var[['var', 'bin', 'totales', 'min_val','max_val']]
	
            # Appendemos los baseline de cada variable
            df_vdi_cuanti_bl=df_vdi_cuanti_bl.append(var)
	
        # Seteamos campos complementarios necesarios para tabla
        df_vdi_cuanti_bl['fecha_foto']=fecha_foto
        df_vdi_cuanti_bl['modelo']= self.nombre_modelo
        df_vdi_cuanti_bl['positivos']= 0
        df_vdi_cuanti_bl['min_p']= 0
        df_vdi_cuanti_bl['max_p']= 0
	
        df_vdi_cuanti_bl=df_vdi_cuanti_bl[['var', 'bin', 'totales', 'positivos','min_p', 'max_p','min_val','max_val', 'fecha_foto', 'modelo']]
	
        # Transformamos a spark
        df_vdi_cuanti_bl =spark.createDataFrame(df_vdi_cuanti_bl)
	
        return df_vdi_cuanti_bl
    
    def __sparkQueryToPandas(self, query):
        queryresult = spark.sql(query)
        return queryresult.toPandas()
         
    def calcular_vdi_cualitativas_baseline(self, fecha_foto, variables, ambiente='sdb_datamining'):
        """Funci??n que calcula el baseline de la distribuci??n de las variables cualitativas.
        
        Inputs:
        -------
            - id: Nombre del campo que identifica el 'id' del modelo por ejemplo el campo 'linea' o el campo 'id_suscripcion'.
            - fecha_foto: fecha en la que se requiere ver la distribuci??n de las variables yyyymmdd.
            - variables: lista de variables a monitorear. No m??s de 20 variables.
            - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo.
            - nombre_modelo: Nombre del modelo a monitorear el performance.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producci??n). 
                        Por defecto 'sdb_datamining'.
        Outputs:
        -------
            - df_vdi_cuali_bl: Dataframe con la distribuci??n baseline de las variables
        """

        warnings.filterwarnings('ignore')
        
        # Validamos que se pasen como par??metro solamente 20 variables
        assert len(variables)<=20, 'Debe insertar 20 variables como m??ximo'
        
        
        periodo = self.__genPeriodo(fecha_foto)
        
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
            from {ambiente}.{self.abt_modelo} 
            where periodo={periodo}"""
        
        
        df_baseline=self.__sparkQueryToPandas(query)
        
        # Generamos el baseline
        ## Definimos dataframe que acumule los baseline de las variables
        df_vdi_cuali_bl=pd.DataFrame()
        
        for variable in variables:
        
            # Calculamos los totales por cada categor??a
            sub = df_baseline[[variable]]
            sub=sub.sort_values(by =[variable])
            tmp_var = sub.groupby([variable])[[variable]].count().rename(columns={variable:"totales"})
        
            # A??adimos el nombre de la variable por la que estamos iterando
            tmp_var['var']=variable
            tmp_var['bin']= tmp_var.index.tolist()
            tmp_var['bin'] = tmp_var['bin'].astype(str)
        
            # Seleccionamos los campos
            var = tmp_var[['bin','var', 'totales']]
        
            # Appendemos los baseline de cada variable
            df_vdi_cuali_bl=df_vdi_cuali_bl.append(var)
        
        # Seteamos campos complementarios necesarios para la tabla
        df_vdi_cuali_bl['fecha_foto']=fecha_foto
        df_vdi_cuali_bl['modelo']= self.nombre_modelo
        df_vdi_cuali_bl['categoria']=df_vdi_cuali_bl['bin']
        df_vdi_cuali_bl['bin']=0
        df_vdi_cuali_bl=df_vdi_cuali_bl[['bin','var','totales','categoria', 'fecha_foto', 'modelo']]
        
        # Transformamos a spark
        df_vdi_cuali_bl =spark.createDataFrame(df_vdi_cuali_bl)
        
        return df_vdi_cuali_bl
    



    def calcular_performance_actual(self, query, fecha_score, cant_bines = 20, ambiente='sdb_datamining'): #TODO: Cant_bines no se usa, revisar
        """
        Funci??n que calcula las m??tricas de performance (para modelos de clasificaci??n binaria) tales como: 
        Information Value, Kolmogorov Smirnov (KS), Gini, AUC, para un periodo en particular, considerando un baseline.
        Tambi??n, crea la tabla de performance (para modelos de clasificaci??n binaria) para un periodo en particular, 
        considerando un baseline, en donde podemos apreciar m??tricas tales como Lift Acumulado, % Captura, % Conversi??n.
        Importante: Enviamos como par??metro el query que joinea el target con el score.
        Inputs:
        -------
            - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo a monitorear el performance.
            - nombre_modelo: Nombre del modelo a monitorear el performance.
            - query: String que contiene el join entre el target y el score del modelo. (Respetar los 'as' en los querys)
            - fecha_score: Fecha en la que se requiere verificar el performance del modelo. Est?? en formato yyyymmdd.
            - cant_bines: Cantidad de bines que se crear?? con la distribuci??n del score. Aplica solo para el baseline. Por defecto 20.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producci??n). 
            Por defecto 'sdb_datamining'.

        Outputs:
        --------
            - df_res_bines, df_res_metricas: Dataframes en spark que contienen la tabla de performance, la tabla con las m??tricas de performance.
        """
        # Importamos librer??as

        warnings.filterwarnings('ignore')

        # Levantamos el join entre el target y la abt
        
        df_bin=self.__sparkQueryToPandas(query)

        # Cargamos el ??ltimo baseline y lo pasamos a pandas
        query=f"""
        select max_p_target from {ambiente}.indicadores_bines 
        where modelo='{self.nombre_modelo}' and tipo='BASELINE' 
        and fecha =(select max(fecha) from {ambiente}.indicadores_bines 
                where modelo='{self.nombre_modelo}' and tipo='BASELINE' )
        order by max_p_target"""
        
        df_baseline=self.__sparkQueryToPandas(query)

        # Ordenamos los puntos de corte
        df_baseline = df_baseline.sort_values(by='max_p_target')

        # Capturamos los puntos de corte
        cut_bin=df_baseline['max_p_target'].tolist()
        cut_bin.insert(0, 0)
        # Asignamos el 1 como la m??xima probabilidad posible
        cut_bin[-1] = 1

        ##################################
        # Tabla de performance
        ##################################

        # Definimos percentiles en base a los puntos de corte del baseline
        df_bin['percentil']=pd.cut(df_bin['score'] , bins=cut_bin)

        # Ordenemos la tabla y definimos los bines
        df_bin=df_bin.sort_values(by=['percentil'])
        bines= pd.DataFrame({'percentil':df_bin.percentil.unique()})
        bines['Bin'] = bines.index
        df_bin=pd.merge(df_bin, bines, how='left', on=['percentil', 'percentil'])

        # Calculamos positivos y totales por grupo
        df_res = df_bin.groupby('percentil').agg({'target':'sum','score':'count'}).reset_index()

        # Calculamos la tasa por grupo
        df_res['porcentaje'] =100* df_res['target']/df_res['score']

        # Capturamos la probabilidad m??xima y m??nima por grupo
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

        # Calculamos lift, porcentaje de captura y porcentaje de conversi??n
        df_res['lift_acum']=(df_res['positivos_acum'] / df_res['totales_acum']) / Tasa
        df_res['porc_captura']=df_res['positivos_acum'] / Positivos_Totales
        df_res['porc_conversion']=df_res['positivos_acum']/df_res['totales_acum']

        # Seteamos campos complementarios necesarios para el dashboard
        df_res['fecha']=fecha_score
        df_res['anio'] = pd.DatetimeIndex(df_res['fecha']).year
        df_res['mes'] = pd.DatetimeIndex(df_res['fecha']).month
        df_res['dia'] = pd.DatetimeIndex(df_res['fecha']).day
        df_res['modelo'] = self.nombre_modelo
        df_res['tipo'] = 'PERFORMANCE'

        df_res['bin']=df_res['Bin'].index.tolist()
        Bin = df_res.pop('Bin') #TODO: Bin no se usa, revisar

        # Ordenamos los campos
        df_res_move=df_res[['anio', 'mes', 'dia', 'bin', 'Positivos', 'Totales', 'min_p_target', 'max_p_target'
                            , 'positivos_acum', 'totales_acum', 'lift_acum',  'porc_captura', 'porc_conversion','fecha','tipo','modelo' ]]

        # Transformamos a spark
        df_res_bines =spark.createDataFrame(df_res_move)

        ##################################
        # Indicadores de performance
        ##################################

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

        # Calculamos m??tricas para determinar el information value, el KS y la curva ROC
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
        df_per['modelo'] = self.nombre_modelo
        df_per['tipo'] = 'PERFORMANCE'

        # Ordenamos los campos
        df_per=df_per[['anio','mes', 'dia','bin',	'total_nro_accts',	'total_porc',	'total_nro_goods',	'good_porc',	'cum_nro_goods',	'cum_porc_goods',	'total_nro_bads',	'bad_porc',	'cum_nro_bads',	'cum_porc_bads',	'intvl_bad_porc',	'cumulative_bad_rate',	'spread_porc',	'actual_odds',	'odds_cumulative',	'informatiovalue',	'pg',	'pb',	'area_rectangulo',	'area_triangulo',	'suma_area',	'fecha','tipo',	'modelo' ]]

        # Transformamos a spark
        df_res_metricas =spark.createDataFrame(df_per)
        
        return df_res_bines, df_res_metricas
    
    
    def calcular_performance_baseline(self, query, fecha_score, cant_bines = 20, ambiente='sdb_datamining'):
        """
        Funci??n que calcula las m??tricas de performance de baseline para los modelos de clasificaci??n binaria, tales como Information Value, Kolmogorov Smirnov (KS), Gini, AUC. 
        Tambi??n, crea la tabla de performance de baseline en donde podemos apreciar m??tricas tales como Lift Acumulado, % Captura, % Conversi??n.
        Importante: Enviamos como par??metro el query que joinea el target con el score.
        Inputs:
        -------
            - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo a monitorear el performance.
            - nombre_modelo: Nombre del modelo a monitorear el performance.
            - query: String que contiene el join entre el target y el score del modelo. (Respetar los 'as' en los querys)
            - fecha_score: Fecha en la que se requiere verificar el performance del modelo. Est?? en formato yyyymmdd.
            - cant_bines: Cantidad de bines que se crear?? con la distribuci??n del score. Aplica solo para el baseline. Por defecto 20.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producci??n). Por defecto 'sdb_datamining'.
        Outputs:
        --------
            - df_res_bines, df_res_metricas, df_psi: Dataframes en spark que contienen la tabla de performance, la tabla con las m??tricas de performance y el baseline del psi, respectivamente.
        """
        warnings.filterwarnings('ignore')

        # Levantamos el join entre el target y la abt
        
        df_bin=self.__sparkQueryToPandas(query)

        # Definimos percentiles en base a la distribuci??n del score
        df_bin['percentil']=pd.qcut(df_bin['score'], cant_bines, duplicates='drop')

        ##################################
        # Tabla de performance
        ##################################

        # Ordenemos la tabla y definimos los bines
        df_bin=df_bin.sort_values(by=['percentil'])
        bines= pd.DataFrame({'percentil':df_bin.percentil.unique()})
        bines['Bin'] = bines.index
        df_bin=pd.merge(df_bin, bines, how='left', on=['percentil', 'percentil'])

        # Calculamos positivos y totales por grupo
        df_res = df_bin.groupby('percentil').agg({'target':'sum','score':'count'}).reset_index()

        # Calculamos la tasa por grupo
        df_res['porcentaje'] =100* df_res['target']/df_res['score']

        # Capturamos la probabilidad m??xima y m??nima por grupo
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

        # Calculamos lift, porcentaje de captura y porcentaje de conversi??n
        df_res['lift_acum']=(df_res['positivos_acum'] / df_res['totales_acum']) / Tasa
        df_res['porc_captura']=df_res['positivos_acum'] / Positivos_Totales
        df_res['porc_conversion']=df_res['positivos_acum']/df_res['totales_acum']

        # Seteamos campos complementarios necesarios para el dashboard
        df_res['fecha']=fecha_score
        df_res['anio'] = pd.DatetimeIndex(df_res['fecha']).year
        df_res['mes'] = pd.DatetimeIndex(df_res['fecha']).month
        df_res['dia'] = pd.DatetimeIndex(df_res['fecha']).day
        df_res['modelo'] = self.nombre_modelo
        df_res['tipo'] = 'BASELINE'

        df_res['bin']=df_res['Bin'].index.tolist()
        Bin = df_res.pop('Bin')#TODO: Bin no se usa, revisar

        # Ordenamos los campos
        df_res_move=df_res[['anio', 'mes', 'dia', 'bin', 'Positivos', 'Totales', 'min_p_target', 'max_p_target'
                            , 'positivos_acum', 'totales_acum', 'lift_acum',  'porc_captura', 'porc_conversion','fecha','tipo','modelo' ]]

        # Transformamos a spark
        df_res_bines =spark.createDataFrame(df_res_move)

        # Insertamos el baseline del PSI, caso contrario, creamos un dataframe de psi vacio.
        df_psi = df_res[['bin', 'max_p_target', 'Totales', 'fecha','tipo','modelo' ]]
        df_psi['tipo'] = cant_bines # en el caso del baseline del psi el tipo hace referencia a la cantidad de bines que usa 
        df_psi = spark.createDataFrame(df_psi)

        ##################################
        # Indicadores de performance
        ##################################

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

        # Calculamos m??tricas para determinar el information value, el KS y la curva ROC
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
        df_per['modelo'] = self.nombre_modelo
        df_per['tipo'] = 'BASELINE'

        # Ordenamos los campos
        df_per=df_per[['anio','mes', 'dia','bin',	'total_nro_accts',	'total_porc',	'total_nro_goods',	'good_porc',	'cum_nro_goods',	'cum_porc_goods',	'total_nro_bads',	'bad_porc',	'cum_nro_bads',	'cum_porc_bads',	'intvl_bad_porc',	'cumulative_bad_rate',	'spread_porc',	'actual_odds',	'odds_cumulative',	'informatiovalue',	'pg',	'pb',	'area_rectangulo',	'area_triangulo',	'suma_area',	'fecha','tipo',	'modelo' ]]

        # Transformamos a spark
        df_res_metricas =spark.createDataFrame(df_per)


        return df_res_bines, df_res_metricas, df_psi


    def calcular_psi(self, fecha_foto, ambiente = 'sdb_datamining'):
        """
        Funci??n que calcula el psi (Population Stability Index) para los modelos de clasificaci??n.
        E inserta en la tabla de psi el calculo del contribution_to_index por bin (el psi es la suma de contribution_to_index)
        Inputs:
        -------
            - id: Nombre del campo que identifica el 'id' del modelo por ejemplo el campo 'linea' o el campo 'id_suscripcion'.
            - score: Nombre del campo que identifica el 'score' del modelo por ejemplo el campo 'score_capro'. 
            - fecha_foto: Fecha en la que se requiere verificar la estabilidad de la poblaci??n scoreada. Est?? en formato yyyymmdd.
            - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo a monitorear la estabilidad.
            - nombre_modelo: Nombre del modelo a monitorear la estabilidad.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producci??n). Por defecto sdb_datamining
        Outputs:
        -------
            - df_ind_PSI: Dataframe con el calculo de contribution_to_index por bin.
        """
        # Importamos librer??as

        # Calculamos el periodo en base a la fecha de la foto de los datos
        
        periodo = self.__genPeriodo(fecha_foto)

        # Cargamos el periodo actual y lo pasamos a pandas
        query =f"""
        select {id} as id ,{self.score} as score from {ambiente}.{self.abt_modelo} where periodo={periodo}
        """
        df_bin_ = spark.sql(query)
        df_bin = df_bin_.toPandas()

        # Cargamos el ??ltimo baseline y lo pasamos a pandas
        query=f"""
        select fecha, bin, max_p_target, totales as baseline_counts 
        from {ambiente}.indicadores_psi_bl
        where modelo='{self.nombre_modelo}'
        and fecha = (select max(fecha) from {ambiente}.indicadores_psi_bl where  modelo='{self.nombre_modelo}') 
        order by bin
        """
        
        df_baseline_psi = self.__sparkQueryToPandas(query)
        # Ordenamos los puntos de corte
        df_baseline_psi = df_baseline_psi.sort_values(by='max_p_target')

        # Capturamos los puntos de corte
        cut_bin=df_baseline_psi['max_p_target'].tolist()
        cut_bin.insert(0, 0)
        # Asignamos el 1 como la m??xima probabilidad posible
        cut_bin[-1] = 1

        # Joineamos baseline con el periodo actual. Calculo del PSI

        ## Definimos percentiles en base a los puntos de corte del baseline
        df_bin['percentil']=pd.cut(df_bin['score'] , bins=cut_bin, duplicates='drop')

        ## Ordenemos la tabla y definimos los bines
        df_bin=df_bin.sort_values(by=['percentil'])
        bines= pd.DataFrame({'percentil':df_bin.percentil.unique()})
        bines['Bin'] = bines.index

        ## A??adimos el campo de bines
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

        # Reemplazamos el 0 por el 1 en el 'validation_counts' para poder calcular el 'ratio' y el resto de m??tricas
        df_ind_PSI['validation_counts'] = np.where(df_ind_PSI['validation_counts']== 0,1,df_ind_PSI['validation_counts'])

        df_ind_PSI['baseline_porc'] = df_ind_PSI['baseline_counts'] / baseline_counts_total
        df_ind_PSI['validation_porc'] = df_ind_PSI['validation_counts'] / validation_counts_total
        df_ind_PSI['difference'] = df_ind_PSI['baseline_porc'] - df_ind_PSI['validation_porc']
        df_ind_PSI['ratio'] = df_ind_PSI['baseline_porc'] / df_ind_PSI['validation_porc']
        df_ind_PSI['weight_of_evidence'] = np.log(df_ind_PSI['ratio']) 
        df_ind_PSI['contribution_to_index'] = df_ind_PSI['difference'] * df_ind_PSI['weight_of_evidence']

        # A??adimos campos para el dashboard de monitoreo
        # Seteamos campos complementarios necesarios para el dashboard
        df_ind_PSI['fecha'] = fecha_foto
        df_ind_PSI['anio'] = pd.DatetimeIndex(df_ind_PSI['fecha']).year
        df_ind_PSI['mes'] = pd.DatetimeIndex(df_ind_PSI['fecha']).month
        df_ind_PSI['dia'] = pd.DatetimeIndex(df_ind_PSI['fecha']).day
        df_ind_PSI['modelo'] = self.nombre_modelo

        # Ordenamos los campos
        df_ind_PSI=df_ind_PSI[['anio','mes','dia', 'Bin',
                                 'baseline_counts', 'baseline_porc', 'validation_counts', 'validation_porc',
                                 'difference', 'ratio', 'weight_of_evidence', 'contribution_to_index','fecha', 'modelo']]

        # Transformamos a spark
        df_ind_PSI_move =spark.createDataFrame(df_ind_PSI)


        return df_ind_PSI_move
    
    def calcular_vdi_cualitativas(self, variables,fecha_foto, ambiente='sdb_datamining' ):
        """
        Funci??n que calcula el vdi para las variables cualitativas.
        Nota: Previamente hay que definir un baseline de las variables cualitativas.
        Inputs:
        -------
            - id: Nombre del campo que identifica el 'id' del modelo por ejemplo el campo 'linea' o el campo 'id_suscripcion'.
            - score: Nombre del campo que identifica el 'score' del modelo por ejemplo el campo 'score_capro'. 
            - variables: lista de variables cualitativas a monitorear (No m??s de 20 variables.)
            - fecha_foto: Fecha en la que se requiere verificar la estabilidad de la poblaci??n scoreada. Est?? en formato yyyymmdd.
            - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo a monitorear la estabilidad.
            - nombre_modelo: Nombre del modelo a monitorear la estabilidad.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producci??n). Por defecto sdb_datamining
        Outputs:
        -------
            - df_vdi_cuali_move: dataframe de spark con el calculo del vdi de cada una de las variables cualitativas.
        """
        # Importamos librerias 
        warnings.filterwarnings('ignore')

        # Validamos que se ingrese la cantidad de variables adecuada
        assert len(variables)<=20 , "No puede insertar m??s de 20 variables. Las variables deben estar definidas en la tabla de baseline"

        # Calculamos el periodo en base a la fecha de la foto de los datos
        
        periodo = self.__genPeriodo(fecha_foto)

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
            ,{self.score} as score
            from {ambiente}.{self.abt_modelo} 
            where periodo={periodo}"""

        
        df_new=self.__sparkQueryToPandas(query)

        # Levantamos el ??ltimo baseline de las variables cualitativas y la pasamos a pandas
        query=f"""
        select var, categoria, totales
        from {ambiente}.indicadores_vdi_cuali_bl
        where modelo ='{self.nombre_modelo}' 
        and fecha= (select max(fecha) from {ambiente}.indicadores_vdi_cuali_bl where modelo ='{self.nombre_modelo}' )
        and var in ({lista_variables})
        """
        
        df_vdi_cuali_bl=self.__sparkQueryToPandas(query)

        # Generamos grupos en base a las categor??as
        ## Definimos dataframes temporales
        df_vdi_cuali_new = pd.DataFrame()
        tmp_var = pd.DataFrame() #TODO: No se usa
        tmp_var_new = pd.DataFrame()

        ## Iteramos por variable
        for variable in variables:

            sub = df_new[['score',variable]]
            count = sub.groupby([variable]).count().rename(columns={variable:"totales"})

            ### Calculamos los totales por categor??a
            tmp_var_new=count.reset_index()
            tmp_var_new=tmp_var_new.rename(columns={variable:"categoria", "score":"totales"})
            tmp_var_new['var']=variable
            var = tmp_var_new[['categoria','var', 'totales']]

            ### Appedeamos los resultados
            df_vdi_cuali_new=df_vdi_cuali_new.append(var)

        # Joineamos Baseline con periodo actual. Luego, calculamos el vdi de cada una de las variables
        ## Pasamos a min??sculas las categor??as tanto en el periodo actual como en el baseline
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

            # Reemplazamos el 0 por el 1 en el 'validation_counts' para poder calcular el 'ratio' y el resto de m??tricas
            df_temp['validation_counts'] = np.where(df_temp['validation_counts']== 0,1,df_temp['validation_counts'])

            df_temp['baseline_porc'] = df_temp['baseline_counts'] / baseline_counts_total
            df_temp['validation_porc'] = df_temp['validation_counts'] / validation_counts_total
            df_temp['difference'] = df_temp['baseline_porc'] - df_temp['validation_porc']
            df_temp['ratio'] = df_temp['baseline_porc'] / df_temp['validation_porc']
            df_temp['weight_of_evidence'] = np.log(df_temp['ratio']) 
            df_temp['contribution_to_index'] = df_temp['difference'] * df_temp['weight_of_evidence']
            var = df_temp[['categoria','var', 'baseline_counts', 'baseline_porc', 'validation_counts', 'validation_porc', 'difference', 'ratio', 'weight_of_evidence', 'contribution_to_index']]

            df_vdi_cuali2=df_vdi_cuali2.append(var)

        # A??adimos campos para el dashboard de monitoreo
        ## Seteamos campos complementarios necesarios para el dashboard
        df_vdi_cuali2['fecha'] = fecha_foto
        df_vdi_cuali2['anio'] = pd.DatetimeIndex(df_vdi_cuali2['fecha']).year
        df_vdi_cuali2['mes'] = pd.DatetimeIndex(df_vdi_cuali2['fecha']).month
        df_vdi_cuali2['dia'] = pd.DatetimeIndex(df_vdi_cuali2['fecha']).day
        df_vdi_cuali2['bin']=''
        df_vdi_cuali2['val_max']=''
        df_vdi_cuali2['modelo'] = self.nombre_modelo
        df_vdi_cuali2['tipo_variable'] = 'cualitativa'

        ## Ordenamos los campos
        df_vdi_cualii=df_vdi_cuali2[['anio','mes','dia','bin','val_max','var',
                                 'baseline_counts', 'baseline_porc', 'validation_counts', 'validation_porc',
                                 'difference', 'ratio', 'weight_of_evidence', 'contribution_to_index','categoria','fecha','modelo','tipo_variable']]

        ## Transformamos a spark e insertamos
        df_vdi_cuali_move =spark.createDataFrame(df_vdi_cualii)

        return df_vdi_cuali_move

    def calcular_vdi_cuantitativas(self, variables,fecha_foto, ambiente='sdb_datamining'):
        """
        Funci??n que calcula el vdi para las variables cuantitativas.
        Nota: Previamente hay que definir un baseline de las variables cuantitativas.
        Inputs:
        -------
            - id: Nombre del campo que identifica el 'id' del modelo por ejemplo el campo 'linea' o el campo 'id_suscripcion'.
            - score: Nombre del campo que identifica el 'score' del modelo por ejemplo el campo 'score_capro'. 
            - variables: lista de variables cuantitativas a monitorear. (hasta 20 variables que deben estar definidas en el baseline)
            - fecha_foto: Fecha en la que se requiere verificar la estabilidad de la poblaci??n scoreada. Est?? en formato yyyymmdd.
            - abt_modelo: Nombre de la tabla donde se encuentra el id y el score del modelo a monitorear la estabilidad.
            - nombre_modelo: Nombre del modelo a monitorear la estabilidad.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producci??n). Por defecto sdb_datamining 
        Outputs:
        --------
            - df_vdi_cuanti_move: dataframe en spark con el calculo del vdi de cada una de las variables cuantitativas.
        """

        
        warnings.filterwarnings('ignore')

        # Validamos que se pasen como par??metro m??ximo 20 variables
        assert len(variables)<=20 , "No puede insertar m??s de 20 variables. Las variables deben estar definidas en la tabla de baseline"

        # Calculamos el periodo en base a la fecha de la foto de los datos
        
        periodo = self.__genPeriodo(fecha_foto)

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
            ,{self.score} as score
            from {ambiente}.{self.abt_modelo} 
            where periodo={periodo}"""

        
        df_actual=self.__sparkQueryToPandas(query)

        # Levantamos el ??ltimo baseline de las variables cualitativas
        query=f"""
        select * from {ambiente}.indicadores_vdi_cuanti_bl 
        where modelo = '{self.nombre_modelo}'
        and fecha = (select max(fecha) from {ambiente}.indicadores_vdi_cuanti_bl where  modelo='{self.nombre_modelo}') 
        and var in ({lista_variables})
        """
        
        df_vdi_cuanti_bl_=self.__sparkQueryToPandas(query)

        ## Nos quedamos con las columnas var, bin y max_val
        df_vdi_cuanti_bl = df_vdi_cuanti_bl_[['var', 'bin', 'min_val', 'max_val']]

        ## Seteamos el indice del dataframe por var (variable)
        df_vdi_cuanti_bl.set_index('var', inplace=True)

        # Generamos grupos en base a las categor??as
        ## Definimos un dataframe temporal
        df_vdi_cuanti = pd.DataFrame()

        #Para todas las variables en variables calculamos la distribuci??n por bin y los valores m??ximos en base al baseline definido
        for variable  in variables:
            # Tomamos el max_val y el bin de la variable
            df_baseline_vdi = df_vdi_cuanti_bl.loc[variable][['min_val','max_val', 'bin']].sort_values(by='bin')

            # Tomamos la columnas max_val y la pasamos a lista
            cut_val=df_baseline_vdi['max_val'].tolist()

            # Definimos el m??nimo valor por variable
            min_val = df_baseline_vdi['min_val'].tolist()[0]-10000 #para incluir el minimo valor

            # A??adimos un valor m??nimo al inicio de la lista
            cut_val.insert(0,min_val)

            # Actualizamos el ??ltimo punto de corte para capturar todos los casos
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

            # Nos quedamos con los valores m??ximos de cada grupo
            max_val = df_val.groupby(['rango']).max().rename(columns={variable:"max_val"})

            # Unificamos los dos dataframes
            count=pd.concat([count_bin,max_val], axis=1,join="inner")

            # A??adimos la columna de la variable
            count['var']= variable

            # A??adimos el bin
            count = count.reset_index()
            count['bin']= count.index

            # Ordenamos los campos
            var = count[['var','bin','max_val', 'totales']]

            # Appendemos las variables
            df_vdi_cuanti=df_vdi_cuanti.append(var)

        # Casteamos a float para asegurar que tenemos el tipo de dato correcto
        df_vdi_cuanti['max_val']=df_vdi_cuanti['max_val'].astype(float)

        # Joineamos Baseline con periodo actual. Luego, calculamos el vdi de cada una de las variables
        df_vdi_cuanti_bl_.pop('max_val') # saco el valor m??ximo del baseline

        ## Joineamos las variables a monitorear con su baseline (df inicial) con un inner join 
        df_ind_VDI = pd.merge(df_vdi_cuanti_bl_, df_vdi_cuanti, on=["var", "bin"])

        ## Colocamos los nombres que corresponden
        df_ind_VDI= df_ind_VDI.rename(columns = {
                                'totales_x':'baseline_counts',
                                'totales_y':'validation_counts'})

        df_ind_VDI=df_ind_VDI.fillna(0) # No deber??a ser necesario

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

            # Reemplazamos el 0 por el 1 en el 'validation_counts' para poder calcular el 'ratio' y el resto de m??tricas
            df_temp['validation_counts'] = np.where(df_temp['validation_counts']== 0,1,df_temp['validation_counts'])

            df_temp['baseline_porc'] = df_temp['baseline_counts'] / baseline_counts_total
            df_temp['validation_porc'] = df_temp['validation_counts'] / validation_counts_total
            df_temp['difference'] = df_temp['baseline_porc'] - df_temp['validation_porc']
            df_temp['ratio'] = df_temp['baseline_porc'] / df_temp['validation_porc']
            df_temp['weight_of_evidence'] = np.log(df_temp['ratio']) 
            df_temp['contribution_to_index'] = df_temp['difference'] * df_temp['weight_of_evidence']
            var = df_temp[['bin','val_max','var', 'baseline_counts', 'baseline_porc', 'validation_counts', 'validation_porc', 'difference', 'ratio', 'weight_of_evidence', 'contribution_to_index']]

            df_vdi_cuanti2=df_vdi_cuanti2.append(var)

        # A??adimos campos para el dashboard de monitoreo
        ## Seteamos campos complementarios necesarios para el dashboard
        df_vdi_cuanti2['fecha'] = fecha_foto
        df_vdi_cuanti2['anio'] = pd.DatetimeIndex(df_vdi_cuanti2['fecha']).year
        df_vdi_cuanti2['mes'] = pd.DatetimeIndex(df_vdi_cuanti2['fecha']).month
        df_vdi_cuanti2['dia'] = pd.DatetimeIndex(df_vdi_cuanti2['fecha']).day
        df_vdi_cuanti2['categoria'] = ''
        df_vdi_cuanti2['tipo_variable'] = 'cuantitativa'
        df_vdi_cuanti2['modelo'] = self.nombre_modelo

        ## Ordenamos los campos
        df_vdi_cuantii=df_vdi_cuanti2[['anio','mes','dia','bin','val_max','var',
                             'baseline_counts', 'baseline_porc', 'validation_counts', 'validation_porc',
                             'difference', 'ratio', 'weight_of_evidence', 'contribution_to_index','categoria','fecha','modelo','tipo_variable']]

        ## Transformamos a spark e insertamos
        df_vdi_cuanti_move =spark.createDataFrame(df_vdi_cuantii)

        return df_vdi_cuanti_move



    def insertar_vdi_cuantitativas_baseline(self, df_vdi_cuanti_bl, ambiente='sdb_datamining'):
        """Funci??n que inserta en hadoop la tabla del baseline de vdi de las variables cuantitativas
        Inputs:
        -------
            - df_vdi_cuanti_bl: Dataframe de spark con el baseline de las variables cuantitativas a insertar en hadoop.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producci??n). Por defecto 'sdb_datamining'.
        """
        
        result = self.__sparkContext()
        self.__insert(df=df_vdi_cuanti_bl, ambiente=ambiente, tabla="indicadores_vdi_cuanti_bl")

    
    def insertar_vdi_cualitativas_baseline(self, df_vdi_cuali_bl, ambiente='sdb_datamining'):
        """Funci??n que inserta en hadoop la tabla del baseline de vdi de las variables cualitativas
        Inputs:
        -------
            - df_vdi_cuali_bl: Dataframe de spark con el baseline de las variables cualitativas a insertar en hadoop.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producci??n). Por defecto 'sdb_datamining'.
        """
        result = self.__sparkContext()
        self.__insert(df=df_vdi_cuali_bl, ambiente=ambiente, tabla="indicadores_vdi_cuali_bl")
    
    def insertar_performance_baseline(self, df_res_bines, df_res_metricas, df_psi, ambiente='sdb_datamining'):
        """Funci??n que inserta en hadoop la tabla de performance de baseline, las m??tricas de performance de baseline y el psi de baseline de un modelo binario 
        Inputs:
        -------
            - df_res_bines: Dataframe en spark que contienen la tabla de performance
            - df_res_metricas: Dataframe en spark que contienen la tabla con las m??tricas de performance
            - df_psi: Dataframe que tiene el baseline del psi.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producci??n). Por defecto 'sdb_datamining'.
        """
        result = self.__sparkContext()
        
        self.__insert(df=df_res_bines, ambiente=ambiente, tabla="indicadores_bines", silent=True)
        self.__insert(df=df_res_metricas, ambiente=ambiente, tabla="indicadores_performance", silent=False)
        self.__insert(df=df_psi, ambiente=ambiente, tabla="indicadores_psi_bl")
    
    def insertar_performance_actual(self, df_res_bines, df_res_metricas, ambiente='sdb_datamining'):
        """Funci??n que inserta en hadoop la tabla de performance y las m??tricas de performance de un modelo 
        Inputs:
        -------
            - df_res_bines: Dataframe en spark que contienen la tabla de performance
            - df_res_metricas: Dataframe en spark que contienen la tabla con las m??tricas de performance
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producci??n). Por defecto 'sdb_datamining'.
        """
        # Levantamos la sesi??n de spark

        result = self.__sparkContext()

        # Insertamos la tabla de performance 
        self.__insert(df=df_res_bines, ambiente=ambiente, tabla="indicadores_bines", silent=False)
        # Insertamos la tabla de m??tricas de performance
        self.__insert(df=df_res_metricas, ambiente=ambiente, tabla="indicadores_performance", silent=False)


    
    def insertar_psi(self, df_psi, ambiente='sdb_datamining'):
        """Funci??n que inserta en hadoop la tabla de psi
        Inputs:
        -------
            - df_psi: Dataframe que tiene el psi del modelo
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producci??n). Por defecto 'sdb_datamining'.
        """
        
        result = self.__sparkContext()
        self.__insert(df=df_psi, ambiente=ambiente, tabla="indicadores_psi")
        
    def insertar_vdi_cuantitativas(self, df_vdi_cuanti, ambiente='sdb_datamining'):
        """Funci??n que inserta en hadoop la tabla de vdi de las variables cuantitativas.
        Inputs:
        -------
            - df_vdi_cuanti: Dataframe que tiene el el vdi de las variables cuantitativas del modelo.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producci??n). Por defecto 'sdb_datamining'.
        """
        result = self.__sparkContext()
                
        self.__insert(df=df_vdi_cuanti, ambiente=ambiente, tabla="indicadores_vdi")
    

    def insertar_vdi(self, df_vdi, ambiente='sdb_datamining'):
        """Funci??n que inserta en hadoop la tabla de vdi de las variables cualitativas/cuantitativas.
        Dado que la estructura es la misma no es necesario tener dos funciones.
        Inputs:
        -------
            - df_vdi: Dataframe que tiene el el vdi de las variables cualitativas del modelo.
            - ambiente: Ambiente en el que se va a guardar el indicador. ('sdb_datamining' es desarrollo / 'data_lake_analytics' es producci??n). Por defecto 'sdb_datamining'.
        """
        result = self.__sparkContext()
        self.__insert(df=df_vdi, ambiente=ambiente, tabla="indicadores_vdi")
        
