

import pandas as pd
import joblib
from predictor_preprocessing import collect_up_to_date_data
import datetime
from datetime import datetime

def start_up_the_machine():

    dataset = pd.read_csv('final_table.csv', sep=';')
    X_test = dataset.drop(labels="sym", axis=1)
    model = joblib.load('model_RF.sav')
    #model = joblib.load('modelRF_no_balanced.sav') #использование другой модели без баланса данных
    prob_preds = model.predict_proba(X_test)
    threshold = 0.2
    symb = pd.Series(dataset['sym'], name='sym')
    proba = pd.DataFrame(prob_preds)
    resultat = pd.concat([symb, proba[1].rename('proba')], axis=1)
    resultat.sort_values(by='proba', inplace=True, ascending=False)

    return resultat.loc[resultat['proba'] > threshold]

    #y_pred = [1 if prob_preds[i][1]> threshold else 0 for i in range(len(prob_preds))]
    #index_list=[i for i,x in enumerate(y_pred) if x==1]
    #print('Количество предсказанных пампов: '+ str(len(index_list)))
    #for i in range(len(index_list)):
    #    symbol = dataset.at[index_list[i], 'sym']
    #    print(symbol + ' ' + str(y_pred[i]))


if __name__ == '__main__':
    question = input('Собрать актуальные данные перед запуском? (д/н)')
    if question == 'д':
        print(datetime.now())
        collect_up_to_date_data()
        print(start_up_the_machine())
    else:
        print(start_up_the_machine())