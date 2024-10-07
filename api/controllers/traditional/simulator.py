from ...extensions import db_uri
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

engine=create_engine(db_uri)


def offer_trad_simulator(codigo_producto, codigo_oficina, precio_propuesto, margen_cadena):
    query = """select a.codigo_producto, a.codigo_oficina, g.ide, a.ind_modelo, e.codigo_linea, a.beta_0, a.beta_1, a.beta_2, a.factor_week, a.sin_modelo, a.caso_borde,
    e.peso_promedio, e.unidad_x_producto, e.estado_producto, d.precio_minimo as precio_base, f.pc, f.costo_por_kilo, f.oc_pesos, f.oc_adim, f.kilos, f.venta,
    g.PRECIO_min, g.PRECIO_max, g.DEMANDA_min, g.DEMANDA_max, g.cantidad_promedio, g.demanda_precio_actual
    from resumen_ind_modelo_trad as a
    join pf_producto as e
    on (a.codigo_producto = e.codigo_producto)
    join (select * from pf_om_lprecio_det_vm where codigo_lista = 'GEN') as d
    on (a.codigo_producto = d.codigo_producto)
    JOIN pf_pc_trad AS f
    ON (a.codigo_producto = f.codigo_producto AND a.codigo_oficina = f.codigo_oficina)
    join rangos_max_trad as g
    on (a.id = g.ide)
    where a.codigo_producto='"""+str(codigo_producto)+"' and a.codigo_oficina = '"+ str(codigo_oficina) +"' and f.pc IS NOT null"
    try:
        df = pd.read_sql(query, engine)

        df.loc[((df['codigo_linea'] > 6)), 'precio_base'] = df['precio_base']/df['unidad_x_producto']
        df.loc[((df['codigo_linea'] <= 6) & (df['estado_producto'] == 'UN')), 'precio_base'] = df['precio_base']/df['peso_promedio']
        df['precio_propuesto'] = precio_propuesto
        
        for i in range(0, len(df)):
            ind_modelo = df.loc[i,'ind_modelo']
            b0 = df.loc[i,'beta_0']
            b1 = df.loc[i,'beta_1']
            b2 = df.loc[i,'beta_2']
            sin_modelo = df.loc[i,'sin_modelo']
            
            p = precio_propuesto
            modelo = 0
            if ind_modelo == 0:
                df.loc[i, 'volumen_propuesto'] = df.loc[i, 'cantidad_promedio']
            else:
                if p < df.loc[i, 'PRECIO_min']:
                    modelo = df.loc[i, 'DEMANDA_max']
                elif p > df.loc[i, 'PRECIO_max']:
                    modelo = df.loc[i, 'DEMANDA_min']
                else:
                    if ind_modelo == 1:
                        p = float(p)
                        modelo = b0 + b1*p
                    elif ind_modelo == 2:
                        p = float(p)
                        modelo = b0 + b1*np.log(p)
                    elif ind_modelo == 3:
                        p = float(p)
                        modelo = np.exp(b0 + b1*np.log(p))
                    elif ind_modelo == 4:
                        p = float(p)
                        modelo = np.exp(b0 + b1*np.log(p) + b2*(np.log(p))**2)
                df.loc[i, 'volumen_propuesto'] = modelo * df.loc[i, 'factor_week']

        df2 = df[['codigo_producto' , 'codigo_linea', 'codigo_oficina', 'peso_promedio', 'estado_producto', 'unidad_x_producto', 'pc', 'costo_por_kilo', 'oc_pesos', 'oc_adim', 'kilos', 'venta', 'volumen_propuesto', 'cantidad_promedio', 'demanda_precio_actual']].groupby(by=['codigo_producto' ,'codigo_linea', 'codigo_oficina', 'peso_promedio', 'estado_producto', 'unidad_x_producto', 'pc', 'costo_por_kilo', 'oc_pesos', 'oc_adim', 'kilos', 'venta']).sum().reset_index()
        df3 = df[['codigo_producto' , 'codigo_linea','codigo_oficina', 'peso_promedio', 'estado_producto', 'unidad_x_producto', 'pc', 'costo_por_kilo', 'oc_pesos', 'oc_adim', 'kilos', 'venta','precio_propuesto','precio_base']].groupby(by=['codigo_producto' , 'codigo_linea','codigo_oficina', 'peso_promedio', 'estado_producto', 'unidad_x_producto', 'pc', 'costo_por_kilo', 'oc_pesos', 'oc_adim', 'kilos', 'venta']).mean().reset_index()
        df = df2.merge(df3, 'inner', on=['codigo_producto', 'codigo_linea','codigo_oficina',  'peso_promedio', 'estado_producto', 'unidad_x_producto', 'pc', 'costo_por_kilo', 'oc_pesos', 'oc_adim', 'kilos', 'venta'])

        volumen_propuesto = df.loc[0, 'volumen_propuesto']
        volumen_propuesto_ro = volumen_propuesto
        precio_propuesto_ro = precio_propuesto
        volumen_propuesto_kg = volumen_propuesto

        if df.loc[0, 'codigo_linea'] > 6:
            peso_promedio = df.loc[0, 'peso_promedio']
            unidad_x_producto = df.loc[0, 'unidad_x_producto']

            precio_unitario = precio_propuesto
            precio_venta = precio_propuesto * df.loc[0, 'unidad_x_producto']
                
            df.loc[0, 'precio_base'] = df.loc[0, 'precio_base'] / unidad_x_producto
            volumen_propuesto_ro = volumen_propuesto * peso_promedio / unidad_x_producto
            precio_propuesto_ro = precio_propuesto * unidad_x_producto / peso_promedio
            volumen_propuesto_kg = volumen_propuesto * peso_promedio / unidad_x_producto
        else:
            if df.loc[0, 'estado_producto'] == 'UN':
                precio_unitario = precio_propuesto * df.loc[0, 'peso_promedio'] / df.loc[0, 'unidad_x_producto']
                precio_venta = precio_unitario * df.loc[0, 'unidad_x_producto']
            else:
                precio_unitario = precio_propuesto
                precio_venta = precio_unitario 
        
        print(precio_propuesto_ro, volumen_propuesto_ro)

        ro_unit = precio_propuesto_ro - df.loc[0, 'costo_por_kilo'] - (df.loc[0, 'oc_pesos'] / df.loc[0, 'kilos']) - ((df.loc[0,'oc_adim'] / df.loc[0, 'venta'])*precio_propuesto_ro)
            
        ro_total = volumen_propuesto_ro * ro_unit
            
        variacion_p_propuesto_base = (precio_propuesto / df.loc[0, 'precio_base']) - 1
        pvp_sugerido = precio_unitario / (1 - margen_cadena)
        ro_propuesto = volumen_propuesto_ro * (precio_propuesto_ro - df.loc[0, 'costo_por_kilo'] - (df.loc[0, 'oc_pesos'] / df.loc[0, 'kilos']) - ((df.loc[0,'oc_adim'] / df.loc[0, 'venta'])*precio_propuesto_ro))
        roskg = (precio_propuesto_ro - df.loc[0, 'costo_por_kilo'] - (df.loc[0, 'oc_pesos'] / df.loc[0, 'kilos']) - ((df.loc[0,'oc_adim'] / df.loc[0, 'venta'])*precio_propuesto_ro))
        pxu_propuesto = precio_unitario
        pv_propuesto = precio_venta


        return {
            "strat_volume": volumen_propuesto,
            "strat_volume_kg": volumen_propuesto_kg,
            "strat_base_variation": variacion_p_propuesto_base,
            "strat_ro": ro_propuesto,
            "recommend_pvp": round(pvp_sugerido),
            "strat_up": pxu_propuesto, 
            "strat_sp": pv_propuesto,
            "unit_ro": ro_unit,
            "total_ro": ro_total,
            "roskg": roskg,
            "product_code": codigo_producto,
            "error": None,
        }
    except Exception as e:
        return {
            "strat_volume": -99,
            "strat_volume_kg": -99,
            "strat_base_variation": -99,
            "strat_ro": -99,
            "recommend_pvp": -99,
            "strat_up": -99, 
            "strat_sp": -99,
            "unit_ro": -99,
            "total_ro": -99,
            "roskg": -99,
            "error": str(e),
            "product_code": codigo_producto
        }