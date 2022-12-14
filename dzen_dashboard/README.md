# Анализ взаимодействия пользователей с карточками Яндекс.Дзен

## Цель исследования  

Нужно ответить на вопросы менеджеров, используя дашборд:  

    Сколько взаимодействий пользователей с карточками происходит в системе с разбивкой по темам карточек?
    Как много карточек генерируют источники с разными темами?
    Как соотносятся темы карточек и темы источников?

Дашбордом предполагается пользоваться не реже, чем раз в неделю, основным пользователем дашборда будут менеджеры по анализу контента.

Состав данных для дашборда: 

    История событий по темам карточек — абсолютные величины с разбивкой по минутам;
    Разбивка событий по темам источников — относительные величины (% событий);
    Таблица соответствия тем источников темам карточек - абсолютные величины;

По каким параметрам данные должны группироваться:

    Дата и время;
    Тема карточки;
    Тема источника;
    Возрастная группа;

## Описание данных   

Источники данных для дашборда: cырые данные о событиях взаимодействия пользователей с карточками

## План исследования

1. Подключение к базе и экспорт данных
2. Подготовка презентации
3. Подготовка дашборда

## Использованы библиотеки

    pandas 
    sqlalchemy

## Результат:  

[Презентация](https://drive.google.com/file/d/19GZZRM3k3DgY3PJ5HOGuMdQM5FUxmWr5/view?usp=sharing)  
[Дашборд](https://public.tableau.com/views/Ya_Dzen_Viz/Dashboard1?:language=en-US&publish=yes&:display_count=n&:origin=viz_share_link)
