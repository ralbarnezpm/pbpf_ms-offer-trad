import pandas as pd

def format_number(number, decimals, brand_code=None):
    try:
        rounded_number = round(number, decimals)
        formatted_number = f"{rounded_number:,.{decimals}f}".replace(',','*').replace('.', ',').replace('*', '.')
        if brand_code:
           if brand_code <= 6:
              return f"{formatted_number} Ton."
           else:
              return f"{formatted_number} Mil U."
        
        return formatted_number
    except Exception as e:
        print(e)
        return pd.NaT
        return ""

def to_number_format(number, decimals):
    try:
      
      # print(f"{round(number, decimals):,}".replace(',','*').replace('.', ',').replace('*', '.'))
      return f"{round(number, decimals):,}".replace(',','*').replace('.', ',').replace('*', '.')
    except Exception as e:
       print(e)
    return ""

def back_to_format(string):
  try:
    return string.replace(".","").replace(",", ".")
  except Exception as e:
    print(e)
  return None
  

def avg_dict(products, field_name):
    sum=0
    for prod in products:
        sum+=int(prod[field_name])
    
    return sum#/len(products)