# This dictionary maps project slug to permitted tables.
#
# Most table permissions are governed by IG, and can be found in the project spreadsheet:
#
# https://docs.google.com/spreadsheets/d/1odgWEwFrkmCr3-7leE2amwVA3b55UCOzbXQOiNgyb1w/edit
#
# However, appointments and wl_* (waitlist) table permissions are not IG
# managed. They are restricted for different reasons, in that there data need
# handling with due attention. Appointments is access managed by Alex, and I do
# not know who decides about waitlist table access.

PERMISSIONS = {
    # project Internal project for curation
    # opensafely/immunosuppressant-meds-research opensafely/with-gp-consultations-curation
    "opensafely-internal": ["icnarc", "appointments"],
    # project 9
    # opensafely/hdruk-os-covid-paeds
    "impact-of-covid-19-on-long-term-healthcare-use-and-costs-in-children-and-young-people": [
        "isaric"
    ],
    # project 87
    # opensafely/renal-short-data-report
    "validation-of-the-opensafely-kidney-codes": ["ukrr"],
    # project 110
    # opensafely/ckd-coverage-ve
    "covid-19-vaccine-coverage-and-effectiveness-in-chronic-kidney-disease-patients": [
        "ukrr"
    ],
    # project 106
    # opensafely/sotrovimab-and-molnupiravir
    "effectiveness-safety-sotrovimab-molnupiravir": ["ukrr"],
    # project 171 (continuation of closed project 137)
    # opensafely/ckd-healthcare-use
    "healthcare-needs-for-people-with-chronic-kidney-disease-in-the-covid-19-era-project-continuation-of-approved-project-no-137": [
        "appointments",
        "ukrr",
    ],
    # project 154
    # opensafely/isaric-exploration
    "validation-of-isaric-sus-phosp-data-for-covid-related-hospital-admissions": [
        "isaric"
    ],
    # project 155
    # opensafely/openprompt-hrqol
    "the-effect-of-long-covid-on-quality-adjusted-life-years-using-openprompt": [
        "open_prompt"
    ],
    # project 122
    # opensafely/waiting-list
    "opioid-prescribing-trends-and-changes-during-covid-19": [
        "wl_openpathways_raw",
        "wl_clockstops_raw",
        "wl_openpathways",
        "wl_clockstops",
    ],
    # project 129
    # opensafely/appointments-short-data-report
    "curation-of-gp-appointments-data-short-data-report": ["appointments"],
    # project 152
    # opensafely/digital-access-to-primary-care
    "digital-access-to-primary-care-for-older-people-during-covid": ["appointments"],
    # project 166, continued from 148
    # opensafely/uom_pregnancy_tx_pathways
    "the-impact-of-covid-19-on-pregnancy-treatment-pathways-and-outcomes-project-continuation-of-approved-project-no-148": [
        "appointments"
    ],
    # project 185
    # opensafely/post-covid-renal opensafely/post-covid-neurodegenerative opensafely/post-covid-cvd opensafely/post-covid-cvd-methods opensafely/post-covid-respiratory
    "investigating-events-following-covid-19": ["appointments"],
    # project 78
    # opensafely/post-covid-kidney-outcomes
    "long-term-kidney-outcomes-after-sars-cov-2-infection": ["appointments"],
    # project 156, continued from project 12
    # opensafely/post-covid-cvd opensafely/post-covid-cvd-methods
    "investigating-events-following-sars-cov-2-infection-project-continuation-of-approved-project-no-12": [
        "appointments"
    ],
    # project 136
    # opensafely/winter-pressures
    "gp-appointments-during-covid": ["appointments"],
    # project 172
    # opensafely/winter-pressures-phase-II
    "impact-and-inequalities-of-winter-pressures-in-primary-care-providing-the-evidence-base-for-mitigation-strategies": [
        "appointments"
    ],
    # project 34
    # opensafely/deaths-at-home-covid19 opensafely/end-of-life-carequality
    "deaths-at-home-during-covid-19": ["appointments"],
    # project 175
    # opensafely/metformin_covid
    "implications-of-metformin-for-long-covid": ["appointments"],
}
