from cohortextractor import StudyDefinition, patients


study = StudyDefinition(
    default_expectations={
        "date": {"earliest": "1970-01-01", "latest": "today"},
        "rate": "uniform",
        "incidence": 0.2,
    },
    population=patients.registered_with_one_practice_between(
        "2019-02-01", "2020-02-01"
    ),
    dereg_date=patients.date_deregistered_from_all_supported_practices(
        on_or_before="2020-12-01",
        date_format="YYYY-MM",
        return_expectations={"date": {"earliest": "2020-02-01"}},
    ),
    a_e_consult_date=patients.attended_emergency_care(
        on_or_after="2020-02-01",
        returning="date_arrived",
        date_format="YYYY-MM-DD",
        find_first_match_in_period=True,
        return_expectations={
            "date": {"earliest": "2020-02-01"},
            "rate": "exponential_increase",
        },
    ),
    icu_date_admitted=patients.admitted_to_icu(
        on_or_after="2020-02-01",
        include_day=True,
        returning="date_admitted",
        find_first_match_in_period=True,
        return_expectations={
            "date": {"earliest": "2020-02-01"},
            "rate": "exponential_increase",
        },
    ),
    died_date_cpns=patients.with_death_recorded_in_cpns(
        on_or_after="2020-02-01",
        returning="date_of_death",
        include_month=True,
        include_day=True,
        return_expectations={
            "date": {"earliest": "2020-02-01"},
            "rate": "exponential_increase",
        },
    ),
    age=patients.age_as_of(
        "2020-02-01",
        return_expectations={
            "rate": "universal",
            "int": {"distribution": "population_ages"},
        },
    ),
    sex=patients.sex(
        return_expectations={
            "rate": "universal",
            "category": {"ratios": {"M": 0.49, "F": 0.51}},
        }
    ),
    imd=patients.address_as_of(
        "2020-02-01",
        returning="index_of_multiple_deprivation",
        round_to_nearest=100,
        return_expectations={
            "rate": "universal",
            "category": {"ratios": {"100": 0.1, "200": 0.2, "300": 0.7}},
        },
    ),
)
