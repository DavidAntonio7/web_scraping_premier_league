from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from multiprocessing import Process
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep

def interagir_objeto(seletor_css,driver,tempo_espera):
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, seletor_css))
    )
    sleep(tempo_espera)
    botao = driver.find_element(By.CSS_SELECTOR, seletor_css)
    botao.click()
    
def Elementos_pagina(driver):
    elementos_com_a_classe = driver.find_elements(By.CSS_SELECTOR, '.more-stats__link')
    elementos = []
    for i in range(0,len(elementos_com_a_classe)):
        elementos_com_a_classe = driver.find_elements(By.CSS_SELECTOR, '.more-stats__link')
        elementos.append(elementos_com_a_classe[i].text)
    return elementos

def Colunas_DF(bs):
    tags_th = bs.find_all('th')
    texto_th = [tag_th.get_text().strip() for tag_th in tags_th if tag_th.get_text().strip() != '']
    texto_th_limpo = texto_th[0:len(list(set(texto_th)))]
    df = pd.DataFrame(columns = texto_th_limpo)
    return df,texto_th_limpo

def Dados_DF(bs,texto_th_limpo,temporada):
    df = pd.DataFrame(columns = texto_th_limpo)
    tags = bs.find_all('td')
    texto = [tag.get_text().strip().replace('.','') for tag in tags if tag.get_text().strip() != '']
    for i in range(0,len(texto_th_limpo)):
        conteudo_organizado = []
        for j in range(0,len(texto),len(texto_th_limpo)):
            if j+i < len(texto):
                conteudo_organizado.append(texto[j+i])
        try:
            df[texto_th_limpo[i]] = list(map(int, conteudo_organizado))
        except:
            df[texto_th_limpo[i]] = conteudo_organizado
    df['Initial Year'] = temporada
    return df
    
def Abrir_Pagina(url):  
    driver = webdriver.Chrome()
    driver.get(url)

    ##tratar pop-ups
    interagir_objeto('#onetrust-accept-btn-handler',driver,3)
    interagir_objeto('#advertClose',driver,3)
    return driver 

def Scrap(elemento_inicial,elemento_final,tempo_de_espera,url, caminho,Player_Or_Club):
    driver = Abrir_Pagina(url)
    contagem_erro = 0
    elementos =  Elementos_pagina(driver)
    for elemento,i in zip(elementos[elemento_inicial:elemento_final],range(elemento_inicial,elemento_final)): ## laco de tipo de estatistica - 13
        print(f'iniciando elemento: {elemento}, i: {i}')
        #try:
        df = pd.DataFrame()
        data_index = 1
        conteudo = ''
        
        sleep(tempo_de_espera)
        elementos_com_a_classe = driver.find_elements(By.CSS_SELECTOR, '.more-stats__link')
        elementos_com_a_classe[i].click()
        sleep(tempo_de_espera)

        texto_th_limpo = ''
        contagem_erro = 0 
        for temporada in (range(2022,1992-1,-1)): ##laco de temporadas
            sleep(tempo_de_espera)
            driver.execute_script("window.scrollTo(0, 0)")
            interagir_objeto('.pageFilter__filter-btn',driver,tempo_de_espera)  ##abre a aba de filtros 

            sleep(tempo_de_espera)
            driver.find_elements(By.CSS_SELECTOR, '.active')[2].click() ##filtro referente a temporada
            sleep(tempo_de_espera)

            driver.find_element(By.XPATH, f'//li[@data-option-index="{data_index}"]').click() ##seleciona a temp
            data_index+=1
            sleep(tempo_de_espera)
            
            interagir_objeto('.filter-button--apply',driver,tempo_de_espera) ##botao apply filters
            
            if Player_Or_Club ==1:
                if temporada <= 1994:         
                    numero_click_next = 3
                else:
                    numero_click_next = 2
            else:
                numero_click_next = 10
            for pagina in range(0,numero_click_next):   ##laco de paginas  
                sleep(tempo_de_espera)
                conteudo = driver.page_source
                bs = BeautifulSoup(conteudo,'html.parser')
                ##nome colunas dataFrame
                try:
                    if (temporada == 2022) & (pagina == 0):                        
                        df,texto_th_limpo = Colunas_DF(bs)
                        df = Dados_DF(bs,texto_th_limpo,temporada)   
                    else:
                        df_aux = Dados_DF(bs,texto_th_limpo,temporada)
                        df = pd.concat([df, df_aux], ignore_index=True)
                        if df_aux.shape[0] < 10:
                            break
                        contagem_erro=0
                    interagir_objeto('.paginationNextContainer',driver,2)
                except:
                    df.to_csv(f'{caminho}{elemento}.csv',encoding='utf-8',index=False)
                    contagem_erro+=1
                    break
                
            if contagem_erro > 3:
                break
             
        df.to_csv(f'{caminho}{elemento}.csv',encoding='utf-8',index=False)
    driver.close()
        
def main():
    control = input('0 para jogador e 1 para clube:  ')
    if control == 0:
        url = 'https://www.premierleague.com/stats/top/players/goals'
        caminho = 'csv_aux/'
        inicio,meio,meio2,fim = 0,13,26,40
        Player_Or_Club = 0
    else:
        url = 'https://www.premierleague.com/stats/top/clubs/wins'
        caminho = 'csv_club_aux/'
        inicio,meio,meio2,fim = 0,12,24,36
        Player_Or_Club = 1
        
    processo1 = Process(target=Scrap, args=(5, 6,5,url,caminho,Player_Or_Club))
    processo2 = Process(target=Scrap, args=(25, 26,5,url,caminho,Player_Or_Club))
    processo3 = Process(target=Scrap, args=(29, 30,5,url,caminho,Player_Or_Club))
    '''processo1 = Process(target=Scrap, args=(inicio, meio,5,url,caminho,Player_Or_Club))
    processo2 = Process(target=Scrap, args=(meio, meio2,5,url,caminho,Player_Or_Club))
    processo3 = Process(target=Scrap, args=(meio2, fim,5,url,caminho,Player_Or_Club))'''

    
    # Iniciar os processos
    processo1.start()
    processo2.start()
    processo3.start()
    # Aguardar até que ambos os processos terminem
    processo1.join()
    processo2.join()
    processo3.join()
    
if __name__ == "__main__":
    main()

#11,10,3,4,9,8,6,7