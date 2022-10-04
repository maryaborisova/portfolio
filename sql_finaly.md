# Анализ базы данных для сервиса чтения книг по подписке.
<a id='start'></a>

# Содержание

1. [Цель исследования](#stage_1)  
    1.1. [Описание данных](#stage_1_1)  
    1.2. [План исследования](#stage_1_2)  
2. [Исследование данных - проверка на пропуски, дубликаты](#stage_2)  
3. [Ответы на вопросы исследования](#stage_3)  
    3.1. [Задача 1](#stage_3_1)  
    3.2. [Задача 2](#stage_3_3)  
    3.3. [Задача 3](#stage_3_5)  
    3.4. [Задача 4](#stage_3_7)   
    3.5. [Задача 5](#stage_3_9)  
4. [Общие выводы и рекомендации](#stage_4)    

## Цель исследования
<a id='stage_1'></a>

Проанализировать базу данных крупного сервиса для чтения книг по подписке. В базе данных содержится информация о книгах, издательствах, авторах, а также пользовательские обзоры книг. Эти данные помогут сформулировать ценностное предложение для нового продукта.

### Описание данных
<a id='stage_1_1'></a>

Таблица books содержит данные о книгах:

    book_id — идентификатор книги;
    author_id — идентификатор автора;
    title — название книги;
    num_pages — количество страниц;
    publication_date — дата публикации книги;
    publisher_id — идентификатор издателя.

Таблица authors содержит данные об авторах:

    author_id — идентификатор автора;
    author — имя автора.

Таблица publishers содержит данные об издательствах:

    publisher_id — идентификатор издательства;
    publisher — название издательства;

Таблица ratings содержит данные о пользовательских оценках книг:

    rating_id — идентификатор оценки;
    book_id — идентификатор книги;
    username — имя пользователя, оставившего оценку;
    rating — оценка книги.

Таблица reviews содержит данные о пользовательских обзорах:

    review_id — идентификатор обзора;
    book_id — идентификатор книги;
    username — имя автора обзора;
    text — текст обзора.

### План исследования:
<a id='stage_1_2'></a>

1. Исследование данных - проверка на пропуски, дубликаты.


2. Ответы на вопросы исследования:

        Посчитайте, сколько книг вышло после 1 января 2000 года;
        Для каждой книги посчитайте количество обзоров и среднюю оценку;
        Определите издательство, которое выпустило наибольшее число книг толще 50 страниц — так вы исключите из анализа брошюры;
        Определите автора с самой высокой средней оценкой книг — учитывайте только книги с 50 и более оценками;
        Посчитайте среднее количество обзоров от пользователей, которые поставили больше 50 оценок.
    
    
3. Общие выводы и рекомендации.

## Исследование данных - проверка на пропуски, дубликаты
<a id='stage_2'></a>
[к содержанию](#start)

Импортируем нужные библиотеки и создадим подключение к базе данных


```python
# импортируем библиотеки
import pandas as pd
from sqlalchemy import create_engine 
```


```python
# устанавливаем параметры
db_config = {'user': 'praktikum_student', # имя пользователя
'pwd': '***', # пароль
'host': '***',
'port': ***, # порт подключения
'db': '***'} # название базы данных
connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_config['user'],
 db_config['pwd'],
 db_config['host'],
 db_config['port'],
 db_config['db']) 
```


```python
# сохраняем коннектор
engine = create_engine(connection_string, connect_args={'sslmode':'require'})
```

Выведем на экран и исследуем датасет с книгами


```python
# формируем sql-запрос.
query = ''' SELECT *
            FROM books
        '''
```


```python
books = pd.io.sql.read_sql(query, con = engine)
books.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>book_id</th>
      <th>author_id</th>
      <th>title</th>
      <th>num_pages</th>
      <th>publication_date</th>
      <th>publisher_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>546</td>
      <td>'Salem's Lot</td>
      <td>594</td>
      <td>2005-11-01</td>
      <td>93</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>465</td>
      <td>1 000 Places to See Before You Die</td>
      <td>992</td>
      <td>2003-05-22</td>
      <td>336</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>407</td>
      <td>13 Little Blue Envelopes (Little Blue Envelope...</td>
      <td>322</td>
      <td>2010-12-21</td>
      <td>135</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>82</td>
      <td>1491: New Revelations of the Americas Before C...</td>
      <td>541</td>
      <td>2006-10-10</td>
      <td>309</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>125</td>
      <td>1776</td>
      <td>386</td>
      <td>2006-07-04</td>
      <td>268</td>
    </tr>
  </tbody>
</table>
</div>




```python
books.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 1000 entries, 0 to 999
    Data columns (total 6 columns):
     #   Column            Non-Null Count  Dtype 
    ---  ------            --------------  ----- 
     0   book_id           1000 non-null   int64 
     1   author_id         1000 non-null   int64 
     2   title             1000 non-null   object
     3   num_pages         1000 non-null   int64 
     4   publication_date  1000 non-null   object
     5   publisher_id      1000 non-null   int64 
    dtypes: int64(4), object(2)
    memory usage: 47.0+ KB
    


```python
type(books['publication_date'])
```




    pandas.core.series.Series



Пропусков в датасете нет, наименования и типы данных корректные. Проверим есть ли явные и неявные дубликаты.


```python
books.duplicated().sum()
```




    0




```python
books[['author_id', 'title', 'num_pages', 'publication_date', 'publisher_id']].duplicated().sum()
```




    0



Дубликатов нет. Посмотрим на статистики датасета, чтобы увидеть максимальные и минимальные цифры количества книг и страниц, идентификаторов авторов и издательств, и сравнить их в дальнейшем с соответствующимии таблицами 


```python
books.describe()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>book_id</th>
      <th>author_id</th>
      <th>num_pages</th>
      <th>publisher_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>count</th>
      <td>1000.000000</td>
      <td>1000.000000</td>
      <td>1000.00000</td>
      <td>1000.000000</td>
    </tr>
    <tr>
      <th>mean</th>
      <td>500.500000</td>
      <td>320.417000</td>
      <td>389.11100</td>
      <td>171.270000</td>
    </tr>
    <tr>
      <th>std</th>
      <td>288.819436</td>
      <td>181.620172</td>
      <td>229.39014</td>
      <td>99.082685</td>
    </tr>
    <tr>
      <th>min</th>
      <td>1.000000</td>
      <td>1.000000</td>
      <td>14.00000</td>
      <td>1.000000</td>
    </tr>
    <tr>
      <th>25%</th>
      <td>250.750000</td>
      <td>162.750000</td>
      <td>249.00000</td>
      <td>83.000000</td>
    </tr>
    <tr>
      <th>50%</th>
      <td>500.500000</td>
      <td>316.500000</td>
      <td>352.00000</td>
      <td>177.500000</td>
    </tr>
    <tr>
      <th>75%</th>
      <td>750.250000</td>
      <td>481.000000</td>
      <td>453.00000</td>
      <td>258.000000</td>
    </tr>
    <tr>
      <th>max</th>
      <td>1000.000000</td>
      <td>636.000000</td>
      <td>2690.00000</td>
      <td>340.000000</td>
    </tr>
  </tbody>
</table>
</div>



Итак, у нас 1000 книг, 636 авторов, 340 издательств. Книги содержат от 14 до 2690 страниц - есть брошюры и объемные тома.

Посмотрим на датасет с авторами.


```python
query = '''SELECT *
           FROM authors
        '''
```


```python
authors = pd.io.sql.read_sql(query, con = engine)
authors.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>author_id</th>
      <th>author</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>A.S. Byatt</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Aesop/Laura Harris/Laura Gibbs</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Agatha Christie</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Alan Brennert</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Alan Moore/David   Lloyd</td>
    </tr>
  </tbody>
</table>
</div>




```python
authors.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 636 entries, 0 to 635
    Data columns (total 2 columns):
     #   Column     Non-Null Count  Dtype 
    ---  ------     --------------  ----- 
     0   author_id  636 non-null    int64 
     1   author     636 non-null    object
    dtypes: int64(1), object(1)
    memory usage: 10.1+ KB
    


```python
authors['author'].duplicated().sum()
```




    0



Датасет без пропусков и дубликатов, наименования и типы данных корректные, содержит 636 авторов, как и датасет с книгами. 

Посмотрим на датасет с издательствами.


```python
query = '''SELECT *
           FROM publishers
        '''
```


```python
publishers = pd.io.sql.read_sql(query, con = engine)
publishers.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>publisher_id</th>
      <th>publisher</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Ace</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Ace Book</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Ace Books</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Ace Hardcover</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Addison Wesley Publishing Company</td>
    </tr>
  </tbody>
</table>
</div>




```python
publishers.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 340 entries, 0 to 339
    Data columns (total 2 columns):
     #   Column        Non-Null Count  Dtype 
    ---  ------        --------------  ----- 
     0   publisher_id  340 non-null    int64 
     1   publisher     340 non-null    object
    dtypes: int64(1), object(1)
    memory usage: 5.4+ KB
    


```python
publishers['publisher'].duplicated().sum()
```




    0



Датасет без пропусков и дубликатов, наименования и типы данных корректные, содержит 340 издательств, как и датасет с книгами.

Посмотрим на датасет с оценками.


```python
query = '''SELECT *
           FROM ratings
        '''
```


```python
ratings = pd.io.sql.read_sql(query, con = engine)
ratings.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>rating_id</th>
      <th>book_id</th>
      <th>username</th>
      <th>rating</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>ryanfranco</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>grantpatricia</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>brandtandrea</td>
      <td>5</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>2</td>
      <td>lorichen</td>
      <td>3</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>2</td>
      <td>mariokeller</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>




```python
ratings.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 6456 entries, 0 to 6455
    Data columns (total 4 columns):
     #   Column     Non-Null Count  Dtype 
    ---  ------     --------------  ----- 
     0   rating_id  6456 non-null   int64 
     1   book_id    6456 non-null   int64 
     2   username   6456 non-null   object
     3   rating     6456 non-null   int64 
    dtypes: int64(3), object(1)
    memory usage: 201.9+ KB
    


```python
ratings.duplicated().sum()
```




    0




```python
ratings[['book_id', 'username', 'rating']].duplicated().sum()
```




    0



Датасет без пропусков и дубликатов, наименования и типы данных корректные, содержит 6456 оценок.

Посмотрим на датасет с отзывами.


```python
query = '''SELECT *
           FROM reviews
        '''
```


```python
reviews = pd.io.sql.read_sql(query, con = engine)
reviews.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>review_id</th>
      <th>book_id</th>
      <th>username</th>
      <th>text</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>brandtandrea</td>
      <td>Mention society tell send professor analysis. ...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>ryanfranco</td>
      <td>Foot glass pretty audience hit themselves. Amo...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>2</td>
      <td>lorichen</td>
      <td>Listen treat keep worry. Miss husband tax but ...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>3</td>
      <td>johnsonamanda</td>
      <td>Finally month interesting blue could nature cu...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>3</td>
      <td>scotttamara</td>
      <td>Nation purpose heavy give wait song will. List...</td>
    </tr>
  </tbody>
</table>
</div>




```python
reviews.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 2793 entries, 0 to 2792
    Data columns (total 4 columns):
     #   Column     Non-Null Count  Dtype 
    ---  ------     --------------  ----- 
     0   review_id  2793 non-null   int64 
     1   book_id    2793 non-null   int64 
     2   username   2793 non-null   object
     3   text       2793 non-null   object
    dtypes: int64(2), object(2)
    memory usage: 87.4+ KB
    


```python
reviews.duplicated().sum()
```




    0




```python
reviews[['book_id', 'username', 'text']].duplicated().sum()
```




    0



Датасет без пропусков и дубликатов, наименования и типы данных корректные, содержит 2793 отзыва.

Вывод: Все датасеты корректные, можно переходить к анализу данных.

## Ответы на вопросы исследования
<a id='stage_3'></a>
[к содержанию](#start)

### Задача 1:
<a id='stage_3_1'></a>

Посчитайте, сколько книг вышло после 1 января 2000 года


```python
query = '''
        SELECT COUNT(book_id) AS books_after_2000
        FROM books
        WHERE publication_date > '2000-01-01'        
        '''
pd.io.sql.read_sql(query, con=engine)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>books_after_2000</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>819</td>
    </tr>
  </tbody>
</table>
</div>



Вывод: После 1 января 2000 года вышло 819 книг из 1000. Подборка в нашей библиотеке достаточно свежая.

### Задача 2:
<a id='stage_3_3'></a>

Для каждой книги посчитайте количество обзоров и среднюю оценку


```python
# отсортируем книги по количеству обзоров
query = '''
        SELECT b.title AS book_title,
               COUNT(DISTINCT rw.review_id) AS review_count,
               AVG(rt.rating) AS average_rating
        FROM books AS b
        LEFT JOIN ratings AS rt ON rt.book_id=b.book_id
        LEFT JOIN reviews AS rw ON rw.book_id=b.book_id
        GROUP BY b.book_id
        ORDER BY review_count DESC,
                 average_rating DESC
        '''
pd.io.sql.read_sql(query, con=engine)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>book_title</th>
      <th>review_count</th>
      <th>average_rating</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Twilight (Twilight  #1)</td>
      <td>7</td>
      <td>3.662500</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Harry Potter and the Prisoner of Azkaban (Harr...</td>
      <td>6</td>
      <td>4.414634</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Harry Potter and the Chamber of Secrets (Harry...</td>
      <td>6</td>
      <td>4.287500</td>
    </tr>
    <tr>
      <th>3</th>
      <td>The Book Thief</td>
      <td>6</td>
      <td>4.264151</td>
    </tr>
    <tr>
      <th>4</th>
      <td>The Glass Castle</td>
      <td>6</td>
      <td>4.206897</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>995</th>
      <td>Disney's Beauty and the Beast (A Little Golden...</td>
      <td>0</td>
      <td>4.000000</td>
    </tr>
    <tr>
      <th>996</th>
      <td>Leonardo's Notebooks</td>
      <td>0</td>
      <td>4.000000</td>
    </tr>
    <tr>
      <th>997</th>
      <td>Essential Tales and Poems</td>
      <td>0</td>
      <td>4.000000</td>
    </tr>
    <tr>
      <th>998</th>
      <td>Anne Rice's The Vampire Lestat: A Graphic Novel</td>
      <td>0</td>
      <td>3.666667</td>
    </tr>
    <tr>
      <th>999</th>
      <td>The Natural Way to Draw</td>
      <td>0</td>
      <td>3.000000</td>
    </tr>
  </tbody>
</table>
<p>1000 rows × 3 columns</p>
</div>




```python
# отсортируем книги по средней оценке
query = '''
        SELECT b.title AS book_title,
               COUNT(DISTINCT rw.review_id) AS review_count,
               AVG(rt.rating) AS average_rating
        FROM books AS b
        LEFT JOIN ratings AS rt ON rt.book_id=b.book_id
        LEFT JOIN reviews AS rw ON rw.book_id=b.book_id
        GROUP BY b.book_id
        ORDER BY average_rating DESC,
                 review_count DESC
        '''
pd.io.sql.read_sql(query, con=engine)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>book_title</th>
      <th>review_count</th>
      <th>average_rating</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A Dirty Job (Grim Reaper  #1)</td>
      <td>4</td>
      <td>5.00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>School's Out—Forever (Maximum Ride  #2)</td>
      <td>3</td>
      <td>5.00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Moneyball: The Art of Winning an Unfair Game</td>
      <td>3</td>
      <td>5.00</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Arrows of the Queen (Heralds of Valdemar  #1)</td>
      <td>2</td>
      <td>5.00</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Wherever You Go  There You Are: Mindfulness Me...</td>
      <td>2</td>
      <td>5.00</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>995</th>
      <td>The World Is Flat: A Brief History of the Twen...</td>
      <td>3</td>
      <td>2.25</td>
    </tr>
    <tr>
      <th>996</th>
      <td>Drowning Ruth</td>
      <td>3</td>
      <td>2.00</td>
    </tr>
    <tr>
      <th>997</th>
      <td>His Excellency: George Washington</td>
      <td>2</td>
      <td>2.00</td>
    </tr>
    <tr>
      <th>998</th>
      <td>Junky</td>
      <td>2</td>
      <td>2.00</td>
    </tr>
    <tr>
      <th>999</th>
      <td>Harvesting the Heart</td>
      <td>2</td>
      <td>1.50</td>
    </tr>
  </tbody>
</table>
<p>1000 rows × 3 columns</p>
</div>



Вывод: Мы можем видеть книги, отсортированные по количеству обзоров и по средней оценке, и выбирать наиболее или наименее популярные. Это может помочь нашим читателям выбирать книги.

### Задача 3:
<a id='stage_3_5'></a>

Определите издательство, которое выпустило наибольшее число книг толще 50 страниц — так вы исключите из анализа брошюры


```python
query = '''
        SELECT pb.publisher,
               COUNT(b.book_id) AS books_count
        FROM books AS b
        LEFT JOIN publishers AS pb ON pb.publisher_id=b.publisher_id
        WHERE b.num_pages > 50
        GROUP BY pb.publisher
        ORDER BY books_count DESC
        LIMIT 1
        '''
pd.io.sql.read_sql(query, con=engine)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>publisher</th>
      <th>books_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Penguin Books</td>
      <td>42</td>
    </tr>
  </tbody>
</table>
</div>



Вывод: Издательство, которое выпустило наибольшее число книг толще 50 страниц - Penguin Books, оно выпустило 42 таких книги. Возможно, за новинками следует прежде всего обращаться к ним. 

### Задача 4:
<a id='stage_3_7'></a>

Определите автора с самой высокой средней оценкой книг — учитывайте только книги с 50 и более оценками



```python
query = '''
        WITH
        books_50 AS (SELECT b.book_id,
                            b.author_id,
                           COUNT(rt.book_id)
                    FROM books AS b
                    LEFT JOIN ratings AS rt ON rt.book_id=b.book_id
                    GROUP BY b.book_id
                    HAVING COUNT(rt.book_id)>=50)

        SELECT a.author,
               AVG(rt.rating) AS average_rating
        FROM books_50 AS b_50
        LEFT JOIN authors AS a ON a.author_id=b_50.author_id
        LEFT JOIN ratings AS rt ON rt.book_id=b_50.book_id
        GROUP BY a.author
        ORDER BY average_rating DESC
        LIMIT 1
        
        '''
pd.io.sql.read_sql(query, con=engine)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>author</th>
      <th>average_rating</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>J.K. Rowling/Mary GrandPré</td>
      <td>4.287097</td>
    </tr>
  </tbody>
</table>
</div>



Вывод: Автор с самой высокой средней оценкой книг (учитывая только книги с 50 и более оценками) - J.K. Rowling/Mary GrandPré

### Задача5:
<a id='stage_3_9'></a>

Посчитайте среднее количество обзоров от пользователей, которые поставили больше 50 оценок


```python
query = '''
        WITH
        users AS (SELECT username,
                         COUNT(rating)
                  FROM ratings
                  GROUP BY username
                  HAVING COUNT(rating)>50)
                  
        SELECT ROUND(AVG(count)) AS average_rewiews
        FROM (SELECT u.username,
                    COUNT(rw.review_id)
             FROM users AS u
             LEFT JOIN reviews AS rw ON rw.username=u.username
             GROUP BY u.username) AS rw_count
        '''
pd.io.sql.read_sql(query, con=engine)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>average_rewiews</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>24.0</td>
    </tr>
  </tbody>
</table>
</div>



Вывод: В базе есть очень активные пользователи - они поставили больше 50 оценок и при этом написали более 24 обзоров.

## Общие выводы и рекомендации
<a id='stage_4'></a>
[к содержанию](#start)

Мы исследовали базу данных крупного сервиса для чтения книг по подписке. Проверили данные на пропуски и дубликаты, а потом ответили на вопросы исследования и получили следующие результаты:

    1. После 1 января 2000 года было издано 819 книг из 1000. Подборка в нашей библиотеке достаточно свежая. Но, возможно, читатели захотят увидеть и классические произведения в нашей библиотеке. Можно расширять ассортимент в эту сторону.
    
    2. Мы можем сортировать книги по количеству обзоров и по средней оценке, и выбирать наиболее или наименее популярные. Это может помочь нашим читателям выбирать книги, а сервису - составлять рекомендации.
    
    3. Издательство, которое выпустило наибольшее число книг толще 50 страниц - Penguin Books, оно выпустило 42 таких книги. Возможно, за новинками следует прежде всего обращаться к ним.
    
    4. Автор с самой высокой средней оценкой книг (учитывая только книги с 50 и более оценками) - J.K. Rowling. Ее средняя оценка 4,3. Похоже, истории про Гарри Поттера и Фантастических тварей мало кого оставляют равнодушным. А учитывая отличную экранизацию - тем более. Это можно учесть в рекомендациях и рекламных акциях.
    
    5. В базе есть очень активные пользователи - они поставили больше 50 оценок и при этом написали более 24 обзоров. Можно разработать систему поощрений для таких пользователей, чтобы стимулировать написание обзоров и выставление оценок.
