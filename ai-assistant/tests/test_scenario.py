"""Single integrated test. Default uses a mock Power BI response; --live uses Ollama and Power BI."""
import argparse
import math
from scenario_interpreter import ScenarioRequest, interpret_scenario
from scenario_engine import run_scenario, extract_value, format_projection

QUESTION = 'If passenger traffic increases by 15% in August 2026, what would be the projected passenger volume?'


def mock_baseline(metric, period):
    assert metric == 'Total Passengers Processed'
    assert period == 'August 2026'
    return extract_value({'results':[{'tables':[{'rows':[{'[Value]':400000}]}]}]})


def live_baseline(metric, period, token):
    from app import resolve_time, build_dax_query, query_powerbi
    if not period:
        raise ValueError('Live test requires an explicit reporting period.')
    periods = resolve_time(period)
    if len(periods) != 1:
        raise ValueError(f'Expected one supported period, got: {period!r}')
    dax = build_dax_query({'supported': True, 'metric':metric,
                           'dimension':None, 'intent':'lookup', 'periods':periods})
    return extract_value(query_powerbi(token, dax))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--live', action='store_true', help='Use local Ollama and real Power BI data')
    args = parser.parse_args()
    if args.live:
        from app import authenticate_powerbi
        scenario = interpret_scenario(QUESTION)
        token = authenticate_powerbi()
        provider = lambda metric, period: live_baseline(metric, period, token)
    else:
        scenario = ScenarioRequest.model_validate({
            'intent':'simulation', 'time_expression':'August 2026',
            'assumptions':[{'metric':'passenger traffic',
                            'operation':'percentage_change', 'value':15}]
        })
        provider = mock_baseline
    results = run_scenario(scenario, provider)
    for result in results:
        print(format_projection(result))
    if not args.live:
        assert math.isclose(results[0].projected, 460000, rel_tol=1e-12)
        print('\nPASS: schema + registry + baseline parsing + projection')

if __name__ == '__main__':
    main()
