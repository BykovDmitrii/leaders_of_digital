from collections import defaultdict

def get_plan(required_OTS, limits, eots):
    print(eots, limits)
    sum_eots = sum([
        eots[billboard][timestamp] for billboard in eots
        for timestamp in eots[billboard]])
    bilboards_estimated_performnce = {
        billboard: required_OTS *
        sum([eots[billboard][timestamp] for timestamp in eots[billboard]]) / sum_eots for billboard in eots}
    shows = dict()
    for bilboard in bilboards_estimated_performnce:
        sum_eots_on_cur_bilboard = sum([eots[bilboard][timestamp] for timestamp in eots[bilboard]])
        estimated_performance_per_day = {}
        stop = False
        shows[bilboard] = defaultdict(int)
        estimated_sum = 0
        while not stop:
            stop = False
            changed = False
            for timestamp in eots[bilboard]: 
                if shows[bilboard][timestamp] < limits[bilboard][timestamp]:
                    shows[bilboard][timestamp] += 6
                    estimated_sum += eots[bilboard][timestamp] * 6.0 / 720
                    changed = True
            if not changed:
                stop = True
            if estimated_sum > sum_eots_on_cur_bilboard:
                stop = True
                for timestamp in sorted(eots, key=lambda x: -eots[bilboard][x]):
                    price = eots[bilboard][timestamp] * 6.0 / 720
                    if shows[bilboard][timestamp] > 0 \
                        and estimated_sum - price > sum_eots_on_cur_bilboard:
                        shows[bilboard][timestamp] -= 6
                        estimated_sum -= price
    return shows
