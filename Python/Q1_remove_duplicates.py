import pandas as pd


if __name__ == '__main__':
    people_1 = pd.read_csv("people_1.txt", sep='\t')
    people_2 = pd.read_csv("people_2.txt", sep='\t')
    people = pd.concat([people_1, people_2])
    people = people.apply(lambda x: x.str.strip())  # removing leading and trailing spaces from all columns
    people['FirstName'] = people['FirstName'].str.capitalize()
    people['LastName'] = people['LastName'].str.capitalize()
    people['Phone'] = people['Phone'].str.replace('-', '')
    people['Address'] = people['Address'].str.replace('#', 'No.')
    people['Address'][~ people['Address'].str.startswith('No.')] = 'No.' + people['Address'][~ people['Address'].str.startswith('No.')]
    people = people.drop_duplicates()
    people.to_csv('people_cleaned.csv', index=False)






