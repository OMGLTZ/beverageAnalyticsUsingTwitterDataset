#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkConf, SparkContext
conf1 = (SparkConf().setMaster("yarn").setAppName("s4604001_ass"))
sc = SparkContext.getOrCreate()
from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark.sql import functions as F
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName("s4604001_ass_session").getOrCreate()


# In[2]:


# the drink lists for 10 different languages
drink_en=[["coffee","americano","espresso","cappuccino","latte","doppio"],["tea"],["milk","juice","chocalate"],
          ["cola","soda","fanta","sprite","lemonade"],["gin","rum","tequila","brandy","cocktail","beer","wine","whiskey","vodka"]]
drink_es=[["café","americano","espresso","capuchino","latte","doppio"],["té"],["leche","jugo","chocalate"],
          ["cola","refresco","fanta","sprite","limonada"],["ginebra","ron","tequila","brandy","cóctel","cerveza","vino","whisky","vodka"]]
drink_pt=[["café","americano","expresso","cappuccino","latte","doppio"],["chá"],["leite","suco","chocalate"],
          ["cola","refrigerante","fanta","sprite","limonada"],["gim","rum","tequila","conhaque","coquetel","cerveja","vinho","whisky","vodka"]]
drink_fr=[["café","américain","expresso","cappuccino","latte","doppio"],["thé"],["lait","jus","chocalate"],
          ["cola","soda","fanta","sprite","limonade"],["gin","rhum","tequila","brandy","cocktail","beer","wine","whisky","vodka"]]
drink_in=[["kopi","americano","espresso","cappuccino","latte","doppio"],["tea"],["susu","jus","chocalate"],
          ["cola","soda","fanta","sprite","limun"],["gin","rum","tequila","brendi","koktail","bir","anggur","wiski","vodka"]]
drink_tr=[["kahve","americano","espresso","cappuccino","latte","doppio"],["çay"],["süt","meyvesuyu","çikolata"],
          ["kola","soda","fanta","sprite","limonata"],["cin","rom","tekila","brendi","kokteyl","bira","şarap","viski","votka"]]
drink_ru=[["кофе","американо","эспрессо","капучино","латте","доппио"],["чай"],
          ["молоко","сок","шоколадныйсок"],
          ["кола","газировка","фанта","спрайт","лимонад"],
          ["джин","ром","текила","бренди","коктейль","пиво","вино","виски’","водка"]]
drink_it=[["caffè","americano","espresso","cappuccino","latte","doppio"],["tè"],["latte","succo","chocalate"],
          ["cola","soda","fanta","sprite","limonata"],["gin","rum","tequila","brandy","cocktail","birra","vino","whisky","vodka"]]
drink_nl=[["koffie","americano","espresso","cappuccino","latte","doppio"],["thee"],["melk","sap","chocalate"],
          ["cola","frisdrank","fanta","sprite","limonade"],["gin","rum","tequila","cognac","cocktail","bier","wijn","whisky","wodka"]]
drink_de=[["kaffee","americano","espresso","cappuccino","latte","doppio"],["tee"],["milch","saft","chocalate"],
          ["cola","soda","fanta","sprite","limonade"],["gin","rum","tequila","brandy","cocktail","bier","wein","whisky","wodka"]]
lang_dict = {'drink_en': drink_en, 'drink_es': drink_es, 'drink_pt': drink_pt, 'drink_fr': drink_fr, 'drink_in': drink_in, 
             'drink_tr': drink_tr, 'drink_ru': drink_ru, 'drink_it': drink_it, 'drink_de': drink_de, 'drink_nl': drink_nl}


# In[3]:


# the mapping drink list UDF
def mapDrink(x, lang):
    drink_list = lang_dict[str('drink_' + lang)]
    drink_t = ""
    for i in range(0, 4):
        for drink in drink_list[i]:
            for word in x:
                if word == drink:
                    if i == 0:
                        drink_t += "coffee,"
                    if i == 1:
                        drink_t += "tea,"
                    if i == 2:
                        drink_t += "healthy drink,"
                    if i == 3:
                        drink_t += "soft drink,"
                    if i == 4:
                        drink_t += "alcohol,"
    if drink_t != "":
        drink_t = drink_t[0: -1]
    return drink_t
mapDrinkUDF = F.udf(lambda x, y: mapDrink(x, y))


# In[4]:


# read dataset, once for a month
json_log = sqlContext.read.json("/data/ProjectDatasetTwitter/statuses.log.2014-10*.gz")


# In[5]:


# filter the language
log_lang = json_log.select('text', 'lang', 'created_at', 'user.utc_offset').filter((json_log.lang == "en") | (json_log.lang == "it") | 
                           (json_log.lang == "es") | (json_log.lang == "pt") | 
                           (json_log.lang == "fr") | (json_log.lang == "in") | 
                           (json_log.lang == "tr") | (json_log.lang == "ru") | 
                           (json_log.lang == "nl") | (json_log.lang == "de"))


# In[7]:


# process the text and hour dimension
log_split = log_lang.withColumn('text_lower', F.lower(F.col('text'))).withColumn('text_split', F.split('text_lower', ' ')).withColumn('created_at_split', F.split('created_at', ' ')[3]).withColumn('hour', F.split('created_at_split', ':')[0]).select('text', 'lang', 'utc_offset', 'text_split', 'hour')


# In[8]:


# using mapping UDF to count drinks
log_map_word = log_split.withColumn("drink_types", mapDrinkUDF(F.col("text_split"), F.col("lang"))).withColumn('drink_type_split', F.split('drink_types', ',')).select('lang', 'utc_offset', 'hour', 'drink_types', 'drink_type_split')


# In[9]:


# groupby the reslut
log_map_word = log_map_word.select('lang', 'utc_offset', 'hour', F.explode('drink_type_split'))
log_result = log_map_word.groupby('col', 'lang', 'utc_offset', 'hour').count()


# In[ ]:


# write the pre-processed data to csv file, once for a month
log_result.write.options(header='True', delimiter=',').csv("/user/s4604001/ass/result_10")

