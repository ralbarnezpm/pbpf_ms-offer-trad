from ...extensions import db_uri
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

engine=create_engine(db_uri)




def offer_simulator(codigo_producto, cadena, precio_propuesto):
    """simulate the proposed price for a product"""

    query = """ select a.*, e.codigo_linea, e.peso_promedio, e.unidad_x_producto, e.estado_producto, round(d.precio_minimo) as precio_base, round(f.pc) as pc,
    g.PRECIO_min, g.PRECIO_max, g.DEMANDA_min, g.DEMANDA_max, round(g.cantidad_promedio) as cantidad_promedio, f.costo_por_kilo, f.oc_pesos as oc, f.kilos, f.venta
    from resumen_ind_modelo as a
    join pf_producto as e
    on (a.codigo_producto = e.codigo_producto)
    join (SELECT a.*, b.cadena from pf_om_lprecio_det_vm AS a JOIN codigo_lista AS b ON (a.codigo_lista = b.codigo_lista)) as d
    on (a.codigo_producto = d.codigo_producto and a.cadena = d.cadena)
    JOIN pf_pc AS f
    ON (a.codigo_producto = f.codigo_producto AND a.CADENA = f.cadena)
    join rangos_max_moderno as g
    on (a.codigo_producto = g.codigo_producto AND a.CADENA = g.cadena)
    where a.codigo_producto='"""+codigo_producto+"' and a.cadena = '"+ cadena +"' and f.pc IS NOT null "

    try:
        df = pd.read_sql(query, engine)
        ind_modelo = df.loc[0, 'IND_MODELO']
        b0 = df.loc[0, 'BETA_0']
        b1 = df.loc[0, 'BETA_1']
        b2 = df.loc[0, 'BETA_2']
        margen_cadena = df.loc[0, 'MARGEN_CADENA']
        factor_week = df.loc[0, 'FACTOR_WEEK']
        precio_propuesto = round(precio_propuesto)
        p = precio_propuesto/(1-margen_cadena)
        if ind_modelo == 0:
            volumen_propuesto = df.loc[0, 'cantidad_promedio']
        else:
            if p < df.loc[0, 'PRECIO_min']:
                modelo = df.loc[0, 'DEMANDA_max']
            elif p > df.loc[0, 'PRECIO_max']:
                modelo = df.loc[0, 'DEMANDA_min']
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
            volumen_propuesto = round(modelo * df.loc[0, 'FACTOR_WEEK'])
        
        pc = df.loc[0, 'pc']
        
        volumen_propuesto_ro = volumen_propuesto
        precio_propuesto_ro = precio_propuesto

        if df.loc[0, 'codigo_linea'] > 6:
            peso_promedio = df.loc[0, 'peso_promedio']
            unidad_x_producto = df.loc[0, 'unidad_x_producto']

            precio_unitario = precio_propuesto
            precio_venta = precio_propuesto * df.loc[0, 'unidad_x_producto']
                
            volumen_propuesto_ro = volumen_propuesto * unidad_x_producto / peso_promedio
            precio_propuesto_ro = precio_propuesto * unidad_x_producto / peso_promedio
        else:
            if df.loc[0, 'estado_producto'] == 'UN':
                precio_unitario = precio_propuesto * df.loc[0, 'peso_promedio'] / df.loc[0, 'unidad_x_producto']
                precio_venta = precio_unitario * df.loc[0, 'unidad_x_producto']
            else:
                precio_unitario = precio_propuesto
                precio_venta = precio_unitario 

        volumen_propuesto_kg = volumen_propuesto
        if df.loc[0, 'codigo_linea'] > 6:
            volumen_propuesto_kg = volumen_propuesto * peso_promedio / unidad_x_producto
        
        variacion_p_propuesto_base = (precio_propuesto / df.loc[0, 'precio_base']) - 1
        pvp_sugerido = precio_unitario / (1 - margen_cadena)
        ro_propuesto = volumen_propuesto_ro * (precio_propuesto_ro - df.loc[0, 'costo_por_kilo'] - (df.loc[0, 'oc'] / df.loc[0, 'kilos']) - ((df.loc[0,'oc'] / df.loc[0, 'venta'])*precio_propuesto_ro))
        roskg = (precio_propuesto_ro - df.loc[0, 'costo_por_kilo'] - (df.loc[0, 'oc'] / df.loc[0, 'kilos']) - ((df.loc[0,'oc'] / df.loc[0, 'venta'])*precio_propuesto_ro))
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
            "roskg": roskg,
            "product_code": codigo_producto,
            "error": None,

        }

    except Exception as e:
        return {
            "strat_volume": -99,
            "strat_base_variation": -99,
            "strat_ro": -99,
            "recommend_pvp": -99,
            "strat_up": -99, 
            "strat_sp": -99,
            "roskg": -99,
            "error": str(e),
            "product_code": codigo_producto
        }