import pandas as pd
from bs4 import BeautifulSoup as bs
from urllib.request import urlopen
import re
import time

"""
This program extracts bibliographical data related from the Bibliographie Papyrologique database.
"""

# The URL of the resulting query is structured in the following way:
URL_partone = 'http://www.aere-egke.be/BP/?fs=2&fs=2&testoTabella=&annee='
# then the year
URL_parttwo = '&cr=&index=&index2=&internet=&n=&nom=&publication=&resume=&sbeseg=&titre=&pag='
# and finally the page

# Compilation of textual patterns to get the data from text
idx_pattern = re.compile('Index')
idxbis_pattern = re.compile('Index\sbis')
titre_pattern = re.compile('Titre')
publication_pattern = re.compile('Publication')
internet_pattern = re.compile('Internet')
resume_pattern = re.compile('Résumé')
sb_seg_pattern = re.compile('S.B. &amp; S.E.G.')
cr_pattern = re.compile('C.R.')
no_pattern = re.compile('Nº')

# We create lists to store all of the data we extract according to the correct category
years = []
indexes = []
indexesbis = []
titres = []
publications = []
internets = []
resumes = []
sb_segs = []
crs = []
nos = []

for URL_year in range(1932,
                      2022):  # looks for publications in all the years in the range, 1932 being the first with results
    print(f'Year: {URL_year}')
    for page in range(1,
                      200):
        bp_URL = (URL_partone + str(URL_year) + URL_parttwo + str(page))  # puts together all parts of the URL
        bp_content = urlopen(bp_URL).read()  # reads the HTML
        bp_soup = bs(bp_content, features='html.parser')  # parses the content
        results = bp_soup.find('div', attrs={'class': 'risultati'}).find_all('table')  # looks for results

        # If there are no results anymore, it breaks the loop and goes on to the next year
        if len(results) == 0:
            break

        # If there are results, it extracts the information from the page
        else:
            for result in results:
                # We first attribute 'Not present' values for all the data we want to extract, if there the data is
                # present, the value of the variable is replaced.

                index = 'Not present'
                indexbis = 'Not present'
                titre = 'Not present'
                publication = 'Not present'
                internet = 'Not present'
                resume = 'Not present'
                sb_seg = 'Not present'
                cr = 'Not present'
                no = 'Not present'

                rows = result.find_all('tr')  # finds every table row (tr) in the table
                rows.pop(0)  # removes the first row, which contains the "Imprimer cette fiche"

                for row in rows:
                    data = row.find_all('td')
                    # Index
                    if re.match(idx_pattern, data[0].text):
                        index = data[1].text.replace('\n', '')

                    # Index bis
                    if re.match(idxbis_pattern, data[0].text):
                        indexbis = data[1].text.replace('\n', '')

                    # Titre
                    if re.match(titre_pattern, data[0].text):
                        titre = data[1].text.replace('\n', '')

                    # Publication
                    if re.match(publication_pattern, data[0].text):
                        publication = data[1].text.replace('\n', '')

                    # Internet
                    if re.match(internet_pattern, data[0].text):
                        internet = data[1].text.replace('\n', '')

                    # Résumé
                    if re.match(resume_pattern, data[0].text):
                        resume = data[1].text.replace('\n', '')

                    # S.B. & S.E.G.
                    if re.match(sb_seg_pattern, data[0].text):
                        sb_seg = data[1].text.replace('\n', '')
                    # C.R.
                    if re.match(cr_pattern, data[0].text):
                        cr = data[1].text.replace('\n', '')
                    # Nº
                    if re.match(no_pattern, data[0].text):
                        no = data[1].text.replace('\n', '')

                # Now we append all the data to the lists we created before
                indexes.append(index)
                indexesbis.append(indexbis)
                titres.append(titre)
                publications.append(publication)
                internets.append(internet)
                resumes.append(resume)
                sb_segs.append(sb_seg)
                crs.append(cr)
                nos.append(no)
                years.append(URL_year)

    if URL_year % 10 == 0:  # for every decade the scraper waits 5 seconds, this is important to avoid being blocked
        # by the server due to repetitive requests
        time.sleep(5)
# checking if every list has the same lenght
print(f'Lenght of all columns:'
    f'{len(indexes)}, {len(indexesbis)}, {len(titres)}, {len(publications)}, '
    f'{len(internets)}, {len(resumes)},'
    f' {len(sb_segs)}, {len(crs)}, {len(nos)} ')

# We can now make a Dataframe with all the data we extracted
df = pd.DataFrame({'Year': years,
                   'Index': indexes,
                   'Index_bis': indexesbis,
                   'Titre': titres,
                   'Publication': publications,
                   'Internet': internets,
                   'Resume': resumes,
                   'SB_SEG': sb_segs,
                   'CR': crs,
                   'No': nos})

df.to_csv('Data/BibliographiePapyrologique.csv', encoding='utf-8')  # let's save it
