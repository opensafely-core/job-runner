from ehrql import create_dataset
from ehrql.tables.core import patients


dataset = create_dataset()

year_of_birth = patients.date_of_birth.year
dataset.define_population(year_of_birth >= 2000)
dataset.year_of_birth = year_of_birth
