# Prueba tecnica para el proceso de carga ETL de un archivo
# Desarrollado por Sergio Silis

import glob
import pandas as pd
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_string_dtype
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from datetime import datetime
from datetime import date
import os
import sqlite3 as sql

# Funciones auxiliares
def norm_data(df):
    num_cols = [cols for cols in df.columns if is_numeric_dtype(df[cols]) and len(df[cols].dropna())>0]
    iter_len_num = len(num_cols)
    string_cols = [cols for cols in df.columns if is_string_dtype(df[cols]) and len(df[cols].dropna())>0]
    iter_len_string = len(string_cols)
    df.dropna(how = 'all')
    
    #para campos numericos
    print('Normalizacion de campos numericos:')
    for x,col_name in enumerate(num_cols):       
        #En campos numericos, reemplazar valores nulos por 0 
        df[col_name] = df[col_name].fillna(0)
        df[col_name] = pd.to_numeric(df[col_name])
        df[col_name] = df[col_name].astype(int)
        print(x+1,' of ',iter_len_num,' completado ',col_name)
        
    #para campos de tipo string    
    print('Normalizacion de campos de tipo cadena:')
    for x,col_name in enumerate(string_cols):        
        #Eliminar espacios en blanco al inicio y al final para cadenas
        df[col_name] = df[col_name].str.strip()
        #Strings en MAYUSCULAS
        df[col_name] = df[col_name].str.upper()
        print(x+1,' of ',iter_len_string,' completado ',col_name)
    return df

def calcula_edad(fecha_nac):
        fecha_nac = datetime.strptime(fecha_nac, "%Y-%m-%d").date()
        today = date.today()
        return today.year - fecha_nac.year - ((today.month, today.day) < (fecha_nac.month, fecha_nac.day))

#extract

def extract(ruta):

    df_union=[]
    files = glob.glob(ruta+'/*')
    for f in files:

        header = ['rut', 'dv', 'nombre',
                'apellido', 'genero', 'fecha_nacimiento',
                'fecha_vencimiento', 'deuda', 'direccion',
                'ocupacion', 'altura', 'peso',             #Clientes
                'correo', 'estatus_contacto', 'telefono','prioridad' #Emails y Telefonos
                ]

        widths = [7,1,20,25,9,10,10,6,50,30,4,2,  #Clientes
                50,8,9,1                         #Emails  y Telefonos                         
                ]

        df = pd.read_fwf(f, names=header,
                            header=None, widths=widths)
        df_union.append(df)
        data = pd.concat(df_union,sort=False,ignore_index=True)
    return data

#transform

def transform(data):
    data = norm_data(data)
    data = data[['rut','dv','nombre','apellido', 'genero', 'fecha_nacimiento','fecha_vencimiento','deuda','direccion','ocupacion','correo','telefono','estatus_contacto','prioridad']]
    data['age'] = data['fecha_nacimiento'].apply(calcula_edad)
    data['age_group'] = pd.cut(x=data['age'], bins=[0,20, 30, 40, 50,60,200], labels=['1','2','3','4','5','6'])
    data['delinquency'] = (datetime.now() - pd.to_datetime(data['fecha_vencimiento'])).dt.days

    #Crear catalogo de clientes con el mayor numero de telefonos validos por ocupacion
    bco_cat = data.loc[data['estatus_contacto'] == 'VALIDO',['ocupacion','rut']].value_counts().reset_index().sort_values(['ocupacion', 0], ascending = (False, False)).drop_duplicates('ocupacion', keep='first')
    data = data.assign(best_contact_ocupation=data.rut.isin(bco_cat.rut).astype(int))
    del bco_cat
    data['fiscal_id'] = data.rut.astype(str) + data.dv.astype(str)

    ## Customer

    customers = data[['fiscal_id','nombre','apellido', 
                        'genero','fecha_nacimiento','age',
                        'age_group','fecha_vencimiento','delinquency',
                        'deuda','direccion','ocupacion',
                        'best_contact_ocupation']].rename(columns = {'nombre': 'first_name',
                                                                        'apellido': 'last_name',
                                                                        'genero': 'gender', 
                                                                        'fecha_nacimiento': 'birth_date',
                                                                        'fecha_vencimiento': 'due_date',
                                                                        'deuda': 'due_balance', 
                                                                        'direccion': 'address',
                                                                        'ocupacion': 'ocupation',
                                                                        })

    # Emails

    emails = data[['fiscal_id','correo','estatus_contacto','prioridad']].rename(columns = {'correo': 'email',
                                                                        'estatus_contacto': 'status',
                                                                        'prioridad': 'priority', 
                                                                        })
    emails.dropna(subset=['email'],inplace = True)
    # Telefonos

    phones = data[['fiscal_id','telefono','estatus_contacto','prioridad']].rename(columns = {'telefono': 'phone',
                                                                        'estatus_contacto': 'status',
                                                                        'prioridad': 'priority', 
                                                                        })
    phones = phones[phones['phone'] > 0]
    
    del data
    
    return customers,emails,phones

#load

def load(customers,emails,phones):
    output_dir = 'output/'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    customers.to_excel(output_dir + 'customers.xlsx',index=False)
    emails.to_excel(output_dir + 'emails.xlsx',index=False)
    phones.to_excel(output_dir + 'phones.xlsx',index=False)

    conn = sql.connect('database.db3')
    customers.to_sql('customers', conn, if_exists='replace',index=False)
    emails.to_sql('emails', conn, if_exists='replace',index=False)
    phones.to_sql('phones', conn, if_exists='replace',index=False)

    conn.close()

    del customers
    del emails
    del phones

if __name__ == "__main__":
    
    print('Por favor capture la ruta donde se encuentran los archivos a procesar:')
    ruta = input()
    print('Inicia lectura de archivo de entrada desde: ' + ruta)
    data = extract(ruta)
    print('Lectura de archivo concluida.\n') 
    print('Inicia proceso de transformacion...')      
    customers,emails,phones = transform(data)
    print('Proceso de transformacion concluido.\n') 
    print('Inicia proceso de carga...')    
    load(customers,emails,phones)
    print('Proceso de carga concluido.') 
    
    
