with source as (
    select *
    from read_files(
        '/Volumes/workspace/hpt_dev/bronze_cms_quality/',
        format => 'json',
        multiLine => true
    )
),

nashville_msa as (
    select cast(ccn as string) as ccn
    from {{ ref('nashville_msa_hospitals') }}
),

renamed as (
    select
        -- Identity
        cast(source.facility_id                                      as string)  as ccn,
        cast(source.facility_name                                    as string)  as facility_name,
        cast(source.hospital_type                                    as string)  as hospital_type,
        cast(source.hospital_ownership                               as string)  as hospital_ownership,
        cast(source.emergency_services                               as string)  as emergency_services,
        cast(source.meets_criteria_for_birthing_friendly_designation as string)  as birthing_friendly,

        -- Overall rating
        try_cast(source.hospital_overall_rating                      as int)     as overall_rating,
        nullif(cast(source.hospital_overall_rating_footnote          as string), '') as overall_rating_footnote,

        -- Mortality
        try_cast(source.mort_group_measure_count                     as int)     as mort_measure_count,
        try_cast(source.count_of_facility_mort_measures              as int)     as mort_facility_count,
        try_cast(source.count_of_mort_measures_better                as int)     as mort_better,
        try_cast(source.count_of_mort_measures_no_different          as int)     as mort_no_different,
        try_cast(source.count_of_mort_measures_worse                 as int)     as mort_worse,
        nullif(cast(source.mort_group_footnote                       as string), '') as mort_footnote,

        -- Safety
        try_cast(source.safety_group_measure_count                   as int)     as safety_measure_count,
        try_cast(source.count_of_facility_safety_measures            as int)     as safety_facility_count,
        try_cast(source.count_of_safety_measures_better              as int)     as safety_better,
        try_cast(source.count_of_safety_measures_no_different        as int)     as safety_no_different,
        try_cast(source.count_of_safety_measures_worse               as int)     as safety_worse,
        nullif(cast(source.safety_group_footnote                     as string), '') as safety_footnote,

        -- Readmissions
        try_cast(source.readm_group_measure_count                    as int)     as readm_measure_count,
        try_cast(source.count_of_facility_readm_measures             as int)     as readm_facility_count,
        try_cast(source.count_of_readm_measures_better               as int)     as readm_better,
        try_cast(source.count_of_readm_measures_no_different         as int)     as readm_no_different,
        try_cast(source.count_of_readm_measures_worse                as int)     as readm_worse,
        nullif(cast(source.readm_group_footnote                      as string), '') as readm_footnote,

        -- Patient experience
        try_cast(source.pt_exp_group_measure_count                   as int)     as pt_exp_measure_count,
        try_cast(source.count_of_facility_pt_exp_measures            as int)     as pt_exp_facility_count,
        nullif(cast(source.pt_exp_group_footnote                     as string), '') as pt_exp_footnote,

        -- Timely and effective care
        try_cast(source.te_group_measure_count                       as int)     as te_measure_count,
        try_cast(source.count_of_facility_te_measures                as int)     as te_facility_count,
        nullif(cast(source.te_group_footnote                         as string), '') as te_footnote

    from source
    inner join nashville_msa
        on cast(source.facility_id as string) = nashville_msa.ccn
)

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

from renamed