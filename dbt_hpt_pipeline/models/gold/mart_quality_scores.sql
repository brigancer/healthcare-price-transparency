with quality_scores as (
    select
        ccn,
        facility_name,
        hospital_type,
        hospital_ownership,
        emergency_services,
        birthing_friendly,
        overall_rating,
        overall_rating_footnote,
        mort_measure_count,
        mort_facility_count,
        mort_better,
        mort_no_different,
        mort_worse,
        mort_footnote,
        safety_measure_count,
        safety_facility_count,
        safety_better,
        safety_no_different,
        safety_worse,
        safety_footnote,
        readm_measure_count,
        readm_facility_count,
        readm_better,
        readm_no_different,
        readm_worse,
        readm_footnote,
        pt_exp_measure_count,
        pt_exp_facility_count,
        pt_exp_footnote,
        te_measure_count,
        te_facility_count,
        te_footnote
    from {{ ref('stg_quality_scores') }}
),

hospitals as (
    select
        ccn,
        hospital_name,
        system,
        city,
        county
    from {{ ref('nashville_msa_hospitals') }}
)

select
    -- Hospital identity
    quality_scores.ccn,
    hospitals.hospital_name,
    hospitals.system,
    hospitals.city,
    hospitals.county,
    quality_scores.facility_name,
    quality_scores.hospital_type,
    quality_scores.hospital_ownership,
    quality_scores.emergency_services,
    quality_scores.birthing_friendly,

    -- Overall rating
    quality_scores.overall_rating,
    quality_scores.overall_rating_footnote,

    -- Mortality
    quality_scores.mort_measure_count,
    quality_scores.mort_facility_count,
    quality_scores.mort_better,
    quality_scores.mort_no_different,
    quality_scores.mort_worse,
    quality_scores.mort_footnote,

    -- Safety
    quality_scores.safety_measure_count,
    quality_scores.safety_facility_count,
    quality_scores.safety_better,
    quality_scores.safety_no_different,
    quality_scores.safety_worse,
    quality_scores.safety_footnote,

    -- Readmissions
    quality_scores.readm_measure_count,
    quality_scores.readm_facility_count,
    quality_scores.readm_better,
    quality_scores.readm_no_different,
    quality_scores.readm_worse,
    quality_scores.readm_footnote,

    -- Patient experience
    quality_scores.pt_exp_measure_count,
    quality_scores.pt_exp_facility_count,
    quality_scores.pt_exp_footnote,

    -- Timely and effective care
    quality_scores.te_measure_count,
    quality_scores.te_facility_count,
    quality_scores.te_footnote

from quality_scores
inner join hospitals
    on quality_scores.ccn = hospitals.ccn