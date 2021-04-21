import pandas as pd
import numpy as np
from datetime import datetime
import calendar


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def clean_df(df:pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
    """
    Função que limpa e transforma o dataframe nos tipos esperados nas outras questões.

    Parâmetros:
    df [pd.core.frame.DataFrame] - um Pandas Dataframe a ser transformado
    
    Retorna:
    Um Pandas Dataframe transformado
    """
    df['DATA INICIAL'] = df['DATA INICIAL'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y'))
    df['DATA FINAL'] = df['DATA FINAL'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y'))
    df['ANO_MES'] = df.apply(lambda x: calcula_regra_ano_mes(x), axis = 1)
    df['PREÇO MÉDIO REVENDA'] = df['PREÇO MÉDIO REVENDA'].apply(lambda x: float(x.replace(',','.')))
    df['PREÇO MÍNIMO REVENDA'] = df['PREÇO MÍNIMO REVENDA'].apply(lambda x: float(x.replace(',','.')))
    df['PREÇO MÁXIMO REVENDA'] = df['PREÇO MÁXIMO REVENDA'].apply(lambda x: float(x.replace(',','.')))

    return df


def calcula_regra_ano_mes(row:pd.core.series.Series) -> str:
    """
    Função que retorna o valor do ano e mês no formato AAAAMM
    Ela possui uma regra que avalia um período semanal e retorna o mês que tem mais dias naquela semana
    Essa função assume que a data final é maior que a inicial e a diferença é de 7 dias

    Parâmetros:
    row [pd.core.series.Series] - o row de um Pandas Dataframe
    
    Retorna:
    Uma valor string no formato AAAAMM
    """
    dt_inicial = row['DATA INICIAL']
    dt_final = row['DATA FINAL']

    #retorna o última/primeiro dia de cada mmês
    ultimo_dia_inicial = calendar.monthrange(int(dt_inicial.strftime('%Y')), int(dt_inicial.strftime('%m')))[1]
    primeiro_dia_final = calendar.monthrange(int(dt_final.strftime('%Y')), int(dt_final.strftime('%m')))[0]

    if  (ultimo_dia_inicial - int(dt_inicial.strftime('%d'))) > ( int(dt_final.strftime('%d')) - primeiro_dia_final ):        
        return dt_inicial.strftime('%Y%m')
    return dt_final.strftime('%Y%m')


def main():

    df = pd.read_csv('SEMANAL_MUNICIPIOS-2019.csv')
    df = clean_df(df)
    
    """
    A) Estes valores estão distribuídos em dados semanais, agrupe eles por mês e calcule as médias de valores de cada combustível por cidade.
    R: Nessa tarefa teve duas questões que tive dúvidas. Uma se refere ao agrupamento por mês.
    Como cada row é por semana e tem uma data inicial e final. No caso das rows que a semana começa em um mês e termina em outro?
    Vejo aqui três soluções:
    a) Usar ou a data inicial ou final como regra
    b) Verificar na semana quantos dias fazem parte de determinado mês e escolher o que tem mais dias
    c) Contar para a média de ambos os meses quando tiver rows com meses diferentes.
    Para esse desafio escolhi a opção acima (b) pois achei mais coerente.
    A outra questão se refere as médias de valores.
    O arquivo possui a média dos valores e o número de postos consultados por semana.
    Há algumas questões a se considerar quando fazermos média de uma média. Se o número de observações for constante não há problemas.
    Mas no caso de por exemplo, uma semana ter 5 postos e na outra ter 10, essa distribuição não é lavada em conta quando fazermos média de uma média.
    Então havia duas formas de fazer essa média:
    a) média dos PREÇO MÉDIO REVENDA semanais.
    b) (PREÇO MÉDIO REVENDA X NÚMERO DE POSTOS PESQUISADOS) para cada semana. No fim dividir pela soma do número de postos.
    Escolhi a alternativa (a) nesse desafio, a fim de simplicidade mas deixamos aqui a possibilidade de fazer B
    """
    df_a = df.groupby(['ESTADO','MUNICÍPIO','ANO_MES','PRODUTO'], as_index=False)[['PREÇO MÉDIO REVENDA']].mean()
    print(df_a)


    """
    B) Calcule a média de valor do combustível por estado e região.
    """
    #Estado
    df_b_estado = df.groupby(['ESTADO','PRODUTO'], as_index=False)[['PREÇO MÉDIO REVENDA']].mean()
    print(df_b_estado)

    #Região
    df_b_regiao = df.groupby(['REGIÃO','PRODUTO'], as_index=False)[['PREÇO MÉDIO REVENDA']].mean()
    print(df_b_regiao)


    """
    C) Calcule a variância e a variação absoluta do máximo, mínimo de cada cidade, mês a mês.
    R: Nessa questão, a interpretação que tive é o cálculo da variância e a variação absoluta do PREÇO MÍNIMO REVENDA e também do PREÇO MÁXIMO REVENDA, 
    ou seja, dois campos separados. A regra do que mês a mês representa segue a mesma explicada na questão A.
    Sei que há a interpretação de que seria a variância e variação absoluta da DIFERENÇA entre os preços mínimos e máximo, mas na minha opinião não fica claro.
    """
    df_c_var = df.groupby(['ESTADO','MUNICÍPIO','ANO_MES','PRODUTO'], as_index=False).agg({'PREÇO MÍNIMO REVENDA': ['var'],'PREÇO MÁXIMO REVENDA': ['var']}) 
    df_c_var.columns = list(map(''.join, df_c_var.columns.values)) #o agregate faz um df multi-index. Aqui transforma de volta em coluna simples
    print(df_c_var)

    df_c_var_abs = df.groupby(['ESTADO','MUNICÍPIO','ANO_MES','PRODUTO'], as_index=False).agg({'PREÇO MÍNIMO REVENDA': ['min','max'],'PREÇO MÁXIMO REVENDA': ['min','max']}) 
    df_c_var_abs.columns = list(map(''.join, df_c_var_abs.columns.values))
    df_c_var_abs['PREÇO MÍNIMO REVENDA VAR ABS'] = df_c_var_abs['PREÇO MÍNIMO REVENDAmax'] - df_c_var_abs['PREÇO MÍNIMO REVENDAmin']
    df_c_var_abs['PREÇO MÁXIMO REVENDA VAR ABS'] = df_c_var_abs['PREÇO MÁXIMO REVENDAmax'] - df_c_var_abs['PREÇO MÁXIMO REVENDAmin']
    print(df_c_var_abs)


    """
    D) Quais são as 5 cidades que possuem a maior diferença entre o combustível mais barato e o mais caro.
    R: Nessa questão, tive duas dúvidas em relação a execução.
    Uma é quanto a range de datas. Uma coisa é considerar que em uma determinada semana/mês qual a maior diferença entre os valores mínimos e máximos.
    Outra é dentre todas as datas. Vou dar um cenário para explicar melhor minha visâo:
    Digamos que uma cidade tenha tido uma crise abastecimento em alguma época. Nesse caso os preços mínimos e máximos dispararam, mas proporcionalmente.
    Então se avaliarmos o ano todo, essa cidade estaria no top 5, porque vamos ter um exemplo de preço mínimo de um mês normal e um exemplo de 
    preço máximo de ums semana com crise, a amplitude dos valores será considerável.
    Enfim, a fim de simplicidade fiz a avaliação do ano todo.
    Outra questão é quanto a agrupar por combustível. Se eu não avaliar o combustível nessa equação todas as primeiras colocações seriam do PRODUTO GLP 
    pois os valores são maiores nesse produto, logo as diferenças também tenderão a ser.
    Eu agrupei por combustível pois achei que faz mais sentido descobrir o top 5 de cada combustível, apesar de não estar claro na questão.
    """    
    df_d = df.groupby(['ESTADO','MUNICÍPIO','PRODUTO'], as_index=False).agg({'PREÇO MÍNIMO REVENDA': ['min'],'PREÇO MÁXIMO REVENDA': ['max']}) 
    df_d.columns = list(map(''.join, df_d.columns.values))
    df_d['PREÇO DIFERENÇA'] = df_d['PREÇO MÁXIMO REVENDAmax'] - df_d['PREÇO MÍNIMO REVENDAmin']

    df_sorted = df_d.sort_values(by=['PRODUTO','PREÇO DIFERENÇA'], ascending=False)

    for produto in df['PRODUTO'].unique():
        df_produto = df_sorted[df_sorted['PRODUTO']==produto]
        print(df_produto[:5])

if __name__ == "__main__":
    main()