import sqlite3
import pandas as pd

con = sqlite3.connect("school.sqlite")

# Запрос № 1 с корректировкой данных.
# Добавить и заполнить все таблицы.

print('Запросы 1 и 2 на добавление/удаление данных...')

con.executescript('''

CREATE TABLE IF NOT EXISTS subjects(
    subject_id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_name VARCHAR(30) NOT NULL
 );

CREATE TABLE IF NOT EXISTS groups(
    group_id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name VARCHAR(10) NOT NULL
 );

CREATE TABLE IF NOT EXISTS cabinets(
    cabinet_id INTEGER PRIMARY KEY AUTOINCREMENT,
    cabinet_name VARCHAR(10) NOT NULL
 );

CREATE TABLE IF NOT EXISTS daysofweek(
    day_of_week_id INTEGER PRIMARY KEY AUTOINCREMENT,
    day_of_week_name VARCHAR(15) NOT NULL
 );

CREATE TABLE IF NOT EXISTS lessons(
    lesson_id INTEGER PRIMARY KEY AUTOINCREMENT,
    time_start TIME NOT NULL
 );

CREATE TABLE IF NOT EXISTS teachers(
    teacher_id INTEGER PRIMARY KEY AUTOINCREMENT,
    teacher_name VARCHAR(50) NOT NULL,
    subject_id INTEGER NOT NULL,
    FOREIGN KEY (subject_id) REFERENCES subjects (subject_id)
 );

CREATE TABLE IF NOT EXISTS subjecthours(
    subjecthours_id INTEGER PRIMARY KEY AUTOINCREMENT,
    count_in_week INTEGER NOT NULL,
    group_id INTEGER NOT NULL,
    subject_id INTEGER NOT NULL,
    teacher_id INTEGER NOT NULL,
    FOREIGN KEY (group_id) REFERENCES groups (group_id),
    FOREIGN KEY (subject_id) REFERENCES subjects (subject_id),
    FOREIGN KEY (teacher_id) REFERENCES teachers (teacher_id)
 );


CREATE TABLE IF NOT EXISTS dayshedule(
    day_of_week_id INTEGER NOT NULL,
    lesson_id INTEGER NOT NULL,
    subjecthours_id INTEGER NOT NULL,
    cabinet_id INTEGER NOT NULL,
    FOREIGN KEY (day_of_week_id) REFERENCES daysofweek (day_of_week_id),
    FOREIGN KEY (lesson_id) REFERENCES lessons (lesson_id),
    FOREIGN KEY (subjecthours_id) REFERENCES subjecthours (subjecthours_id),
    FOREIGN KEY (cabinet_id) REFERENCES cabinets (cabinet_id)
 );
 
 
 
 
 
 
 
 
 
 
INSERT INTO subjects (subject_name)
VALUES
('Русский язык'),
('Математика'),
('Окружающий мир'),
('Физическая культура'),
('Литература'),
('История'),
('Физика'),
('Изобразительное искусство'),
('Химия'),
('История'),
('Музыка'),
('Основы безопасности жизнедеятельности'),
('География'),
('Технология');
 
INSERT INTO groups (group_name)
VALUES
('1А'),
('1Б'),
('2'),
('3'),
('4'),
('5А'),
('5Б'),
('5В'),
('6'),
('7'),
('8'),
('9'),
('11А'),
('11Б');

 
INSERT INTO cabinets (cabinet_name)
VALUES
('101'),
('102'),
('107'),
('204'),
('205'),
('211'),
('302'),
('303'),
('303a'),
('303b');
 
INSERT INTO daysofweek (day_of_week_name)
VALUES
('Понедельник'),
('Вторник'),
('Среда'),
('Четверг'),
('Пятница'),
('Суббота');
 
INSERT INTO lessons (time_start)
VALUES
('08:30:00'),
('09:25:00'),
('10:20:00'),
('11:15:00'),
('12:10:00'),
('13:05:00'),
('14:00:00'),
('14:55:00');
 
INSERT INTO teachers (teacher_name, subject_id)
VALUES
('Иванова Марина Владимировна', '1'),
('Петрова Елена Ивановна', '1'),
('Сидорова Елена Николаевна', '2'),
('Николаева Анна Игоревна', '3'),
('Иванова Мария Петровна', '4'),
('Иванова Дарья Александровна', '5'),
('Петрова Софья Николаевна', '6'),
('Иванов Иван Иванович', '7'),
('Никулина Елена Ивановна', '8'),
('Маринина Валерия Петровна', '9'),
('Пахомов Николай Юрьевич', '10'),
('Василенко Юрий Петрович', '11'),
('Сельцов Иван Пахомович', '12'),
('Сельцова Альбина Николаевна', '13'),
('Петренко Владислав Владиславович', '14'),
('Иванов Владимир Петрович', '14');

 
INSERT INTO subjecthours (group_id, subject_id, teacher_id, count_in_week)
VALUES
('1', '1', '1', '4'),
('1', '2', '3', '4'),
('1', '3', '4', '3'),
('1', '4', '5', '2'),
('1', '5', '6', '3'),
('1', '8', '9', '1'),
('1', '11', '12', '1'),

('3', '1', '2', '5');

INSERT INTO dayshedule (day_of_week_id, lesson_id, subjecthours_id, cabinet_id)
VALUES
('1', '1', '1', '1'),
('1', '2', '2', '2'),
('1', '3', '3', '2'),
('1', '4', '8', '1'),

('2', '1', '1', '3'),
('2', '2', '1', '3'),
('2', '3', '2', '3'),

('3', '3', '1', '1'),
('3', '4', '2', '1'),
('3', '5', '2', '1'),

('4', '2', '3', '1'),
('4', '3', '4', '2'),
('4', '5', '5', '3'),
('4', '6', '11', '4'),
('4', '7', '4', '2'),

('5', '1', '3', '5'),
('5', '2', '5', '5'),
('5', '4', '5', '1');







 ''')

con.commit()


cursor = con.cursor()

print()


# Запрос № 2 с корректировкой данных.
# Удалить из базы данных все 10 и 11 классы.

con.executescript('''

DELETE FROM groups
WHERE group_name LIKE '%10%' OR group_name LIKE '%11%';

 ''')
con.commit()



print('----------------------------------------------')


# Запрос № 1 для связанных таблиц на выборку с условиями и сортировкой
# Найти число занятий по каждому предмету для класса 1А.
print('Запрос № 1 для связанных таблиц на выборку с условиями и сортировкой')
df = pd.read_sql(f'''
 SELECT 
 group_name AS Класс, 
 subject_name AS Предмет, 
 teacher_name AS Учитель, 
 count_in_week AS Число
 FROM subjecthours
 JOIN groups USING (group_id)
 JOIN subjects USING (subject_id)
 JOIN teachers USING (teacher_id)
 WHERE group_name = :classname 
 ORDER BY group_name
''', con, params={"classname": '1А'})
print(df)


print()




# Запрос № 2 для связанных таблиц на выборку с условиями и сортировкой
# Вывести расписание на четверг для класса 1А.
print('Запрос № 2 для связанных таблиц на выборку с условиями и сортировкой')
df = pd.read_sql(f'''
 SELECT 
 lesson_id AS Урок,
 time_start AS ВремяНачала,
 subject_name AS Предмет,
 teacher_name AS Учитель,
 cabinet_name AS Кабинет
 FROM dayshedule
 JOIN lessons USING (lesson_id)
 JOIN subjecthours USING (subjecthours_id)
 JOIN teachers USING (teacher_id)
 JOIN subjects USING (subject_id)
 JOIN cabinets USING (cabinet_id)
 JOIN groups USING (group_id)
 JOIN daysofweek USING (day_of_week_id)
 WHERE group_name = :classname 
 AND day_of_week_id = :userday 
 ORDER BY time_start
''', con, params={"classname": '1А', "userday": '4'})
print(df)

print('----------------------------------------------')

# Запрос № 1 на группировку и групповые функции
# Найти, сколько всего в неделю есть занятий по каждому предмету в школе, по которому проводятся занятия.
print('Запрос № 1 на группировку и групповые функции')
df = pd.read_sql(f'''
 SELECT 
 subject_name AS Предмет, 
 SUM (count_in_week) AS ВСЕГО_ЗАНЯТИЙ
 FROM subjecthours
 JOIN subjects USING (subject_id)
 GROUP BY subject_id
''', con, params={})
print(df)

print()

# Запрос № 2 на группировку и групповые функции
# Найти, сколько всего в школе есть преподавателей каждого предмета.
print('Запрос № 2 на группировку и групповые функции')
df = pd.read_sql(f'''
 SELECT 
 subject_name AS Предмет,
 COUNT (teacher_id) AS ВСЕГО_УЧИТЕЛЕЙ_КАЖДОГО_ПРЕДМЕТА
 FROM teachers
 JOIN subjects USING (subject_id)
 GROUP BY subject_id
''', con, params={})
print(df)


print('----------------------------------------------')

# Запрос № 1 с вложенными запросами или табличными выражениями
# Найти всех преподавателей предметов, у которых ID <= 5.
print('Запрос № 1 с вложенными запросами или табличными выражениями')
df = pd.read_sql(f'''
 SELECT 
 teacher_name AS Учитель,
 subject_name AS Предмет
 FROM teachers
 JOIN subjects USING (subject_id)
 WHERE subject_name IN (
    SELECT subject_name 
    FROM subjects
    WHERE subject_id <= 5
 )
''', con, params={})
print(df)

print()


# Запрос № 2 с вложенными запросами или табличными выражениями
# Найти дни и время всех занятий, которые проходят в кабинетах, в чьих названиях есть цифра 5.
print('Запрос № 2 с вложенными запросами или табличными выражениями')
df = pd.read_sql(f'''
 SELECT 
 day_of_week_name AS День_недели,
 time_start AS Время_начала,
 cabinet_id AS ID_кабинета
 FROM dayshedule
 JOIN daysofweek USING (day_of_week_id)
 JOIN lessons USING (lesson_id)
 WHERE cabinet_id IN (
    SELECT cabinet_id
    FROM cabinets
    WHERE cabinet_name LIKE "%5%"
 )
''', con, params={})
print(df)

con.close()