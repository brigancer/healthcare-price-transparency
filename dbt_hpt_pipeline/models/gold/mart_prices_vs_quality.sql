with median_prices as (
    select
        ccn,
        code_1,
        code_1_type,
        service_description,
        category,
        standard_charge_discounted_cash,
        standard_charge_gross,
        standard_charge_min,
        standard_charge_max,
        percentile_cont(0.5) within group (order by negotiated_dollar) as median_negotiated_dollar
    from {{ ref('mart_hospital_prices') }}
    where negotiated_dollar is not null
    group by
        ccn,
        code_1,
        code_1_type,
        service_description,
        category,
        standard_charge_discounted_cash,
        standard_charge_gross,
        standard_charge_min,
        standard_charge_max
),

quality_scores as (
    select
        ccn,
        hospital_name,
        system,
        city,
        county,
        overall_rating,
        overall_rating_footnote,
        mort_better,
        mort_no_different,
        mort_worse,
        safety_better,
        safety_no_different,
        safety_worse,
        readm_better,
        readm_no_different,
        readm_worse,
        pt_exp_facility_count,
        pt_exp_footnote,
        te_facility_count,
        te_footnote
    from {{ ref('mart_quality_scores') }}
)

select
    -- Hospital identity
    median_prices.ccn,
    quality_scores.hospital_name,
    quality_scores.system,
    quality_scores.city,
    quality_scores.county,

    -- Service identity
    median_prices.code_1,
    median_prices.code_1_type,
    median_prices.category,
    median_prices.service_description,

    -- Prices
    median_prices.median_negotiated_dollar,
    median_prices.standard_charge_discounted_cash,
    median_prices.standard_charge_gross,
    median_prices.standard_charge_min,
    median_prices.standard_charge_max,

    -- Overall quality
    quality_scores.overall_rating,
    quality_scores.overall_rating_footnote,

    -- Mortality
    quality_scores.mort_better,
    quality_scores.mort_no_different,
    quality_scores.mort_worse,

    -- Safety
    quality_scores.safety_better,
    quality_scores.safety_no_different,
    quality_scores.safety_worse,

    -- Readmissions
    quality_scores.readm_better,
    quality_scores.readm_no_different,
    quality_scores.readm_worse,

    -- Patient experience
    quality_scores.pt_exp_facility_count,
    quality_scores.pt_exp_footnote,

    -- Timely and effective care
    quality_scores.te_facility_count,
    quality_scores.te_footnote

from median_prices
inner join quality_scores
    on median_prices.ccn = quality_scores.ccn