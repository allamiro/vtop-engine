"""The projected run time is a bound the run can keep (the v0.11.1 AWS round's lesson 2).

`plan.check_budget` used to charge a fixed-rate leg `fixed_seconds` and a saturation or consume run its bytes
at a 2 MiB/s floor. Neither held in the AWS round: a leg ran 7 to 20 minutes where a system could not hold
the rate, and 100 B records at VTOP's ~6.2k records/s move 0.6 MiB/s, so the projection was off by hours.
Now a leg is charged its wall limit (`3 x fixed_seconds + 60 s`), a sized run the longer of its bytes at
2 MiB/s and its records at `SLOWEST_RECORDS_PER_SECOND`, and the summary also prints the figure if every
fixed-rate leg is on time.
"""
from kafka_compare import plan

# The AWS round's shape: 100 B only, 3 M records per saturation and consume run, 3 repetitions, 30 s legs.
AWS_100B = {"record_sizes": [100], "partitions": [1], "producers": [1], "compressions": ["none"], "acks": ["all"],
            "profile": "defaults", "profiles": ["defaults"], "repetitions": 3, "load_pcts": [25, 50, 75],
            "load_bases": {"kafka": [25, 50, 75]}, "consume": True, "sat_records": {"100": 3_000_000},
            "fixed_seconds": 30.0, "warmup_fraction": 0.2, "smoke_records": 10000}


def test_the_aws_round_s_100_b_matrix_is_charged_what_vtop_took():
    # Per system: 3 repetitions x (a saturation and a consume run of 3 M records at no more than VTOP's ~6.2k
    # records/s, and three legs at their wall limit). The old formula gave 2677 s for both systems.
    per_rep = 2 * 3_000_000 / 6_200 + 3 * (3 * 30 + 60)
    assert plan.projected_seconds(AWS_100B) >= 2 * 3 * per_rep


def test_a_sized_run_is_charged_by_its_records_at_the_floor_when_that_is_longer_than_its_bytes():
    records = 3_000_000
    by_bytes = records * 100 / plan.SLOWEST_BYTES_PER_SECOND
    by_records = records / plan.SLOWEST_RECORDS_PER_SECOND
    assert by_records > by_bytes
    rated = 3 * (plan.DEFAULT_TIMING["fixed_rate"] + plan.fixed_rate_wall_limit(30.0))
    one = plan.DEFAULT_TIMING["bring_up"] + plan.DEFAULT_TIMING["warm_up"] + 3 * (2 * by_records + rated)
    assert plan.projected_seconds(AWS_100B) == 2 * one


def test_the_record_floor_is_no_faster_than_the_slowest_system_measured():
    # VTOP at 100 B in the v0.11.1 AWS round: about 6.2k records/s.
    assert 0 < plan.SLOWEST_RECORDS_PER_SECOND <= 6_200


def test_the_on_time_figure_charges_each_leg_its_target():
    bound = plan.projected_seconds(AWS_100B)
    on_time = plan.projected_seconds(AWS_100B, on_time=True)
    legs = 2 * 3 * 3  # systems x repetitions x rates
    assert bound - on_time == legs * (plan.fixed_rate_wall_limit(30.0) - 30.0)


def test_the_summary_prints_the_bound_and_the_on_time_figure():
    text = plan.summary(AWS_100B)
    bound = plan.projected_seconds(AWS_100B) / 3600
    on_time = plan.projected_seconds(AWS_100B, on_time=True) / 3600
    assert f"projected {bound:.1f} h" in text
    assert f"{on_time:.1f} h if every fixed-rate leg is on time" in text


def test_the_budget_refuses_on_the_bound():
    bound = plan.projected_seconds(AWS_100B) / 3600
    on_time = plan.projected_seconds(AWS_100B, on_time=True) / 3600
    limit = (bound + on_time) / 2
    try:
        plan.check_budget(AWS_100B, limit)
    except plan.PlanRefused as exc:
        assert f"{bound:.1f} h" in str(exc)
    else:
        raise AssertionError(f"a matrix bounded at {bound:.2f} h passed a {limit:.2f} h budget")
