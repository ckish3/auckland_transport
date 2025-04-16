
import json
import pandas as pd

import download_data as download_data
import trip_update as trip_update

def test_convert_request_to_trip_update():
    input = """
    {"response": {
        "header": {
            "timestamp": 1744485168.859,
            "gtfs_realtime_version": "1.0",
            "incrementality": 0
        },
        "entity": [{
            "id": "902-98012-34200-1-063671ab",
            "trip_update": {
                "trip": {
                    "trip_id": "902-98012-34200-1-063671ab",
                    "start_time": "09:30:00",
                    "start_date": "20250413",
                    "schedule_relationship": 3,
                    "route_id": "MTIA-209",
                    "direction_id": 1
                },
                "timestamp": 1744452776,
                "delay": 0
            },
            "is_deleted": false
        }, {
            "id": "902-98011-37800-1-d2486685",
            "trip_update": {
                "trip": {
                    "trip_id": "902-98011-37800-1-d2486685",
                    "start_time": "10:30:00",
                    "start_date": "20250413",
                    "schedule_relationship": 3,
                    "route_id": "MTIA-209",
                    "direction_id": 0
                },
                "timestamp": 1744452798,
                "delay": 0
            },
            "is_deleted": false
        }, {
            "id": "1397-38017-18600-2-1c8a525c",
            "trip_update": {
                "trip": {
                    "trip_id": "1397-38017-18600-2-1c8a525c",
                    "start_time": "05:10:00",
                    "start_date": "20250413",
                    "schedule_relationship": 0,
                    "route_id": "AIR-221",
                    "direction_id": 0
                },
                "stop_time_update": {
                    "stop_sequence": 24,
                    "arrival": {
                        "delay": 1642,
                        "time": 1744482442,
                        "uncertainty": 0
                    },
                    "stop_id": "1808-0d9a1f7f",
                    "schedule_relationship": 0
                },
                "vehicle": {
                    "id": "26008",
                    "label": "GB8904",
                    "license_plate": "NFS279"
                },
                "timestamp": 1744482459,
                "delay": 1642
            },
            "is_deleted": false
        }, {
            "id": "1021-11147-19560-2-4ce78e52",
            "trip_update": {
                "trip": {
                    "trip_id": "1021-11147-19560-2-4ce78e52",
                    "start_time": "05:26:00",
                    "start_date": "20250413",
                    "schedule_relationship": 0,
                    "route_id": "RBW-402",
                    "direction_id": 0
                },
                "stop_time_update": {
                    "stop_sequence": 17,
                    "arrival": {
                        "delay": -499,
                        "time": 1744483481,
                        "uncertainty": 0
                    },
                    "stop_id": "7016-875f30cb",
                    "schedule_relationship": 0
                },
                "vehicle": {
                    "id": "23633",
                    "label": "PC4633",
                    "license_plate": "KPK176"
                },
                "timestamp": 1744484818,
                "delay": -499
            },
            "is_deleted": false
        }]
    }
}"""

    expected = [{
            "trip_id": "1397-38017-18600-2-1c8a525c",
            "start_time": "05:10:00",
            "start_date": "20250413",
            "route_id": "AIR-221",
            "direction_id": 0,
            "stop_sequence": 24,
            "stop_delay": 1642,
            "stop_time": 1744482442,
            "stop_uncertainty": 0,
            "stop_id": "1808-0d9a1f7f",
            "vehicle_id": "26008",
            "vehicle_license_plate": "NFS279",
            "timestamp": 1744482459,
            "trip_delay": 1642
        },
        {
            "trip_id": "1021-11147-19560-2-4ce78e52",
            "start_time": "05:26:00",
            "start_date": "20250413",
            "route_id": "RBW-402",
            "direction_id": 0,
            "stop_sequence": 17,
            "stop_delay": -499,
            "stop_time": 1744483481,
            "stop_uncertainty": 0,
            "stop_id": "7016-875f30cb",
            "vehicle_id": "23633",
            "vehicle_license_plate": "KPK176",
            "timestamp": 1744484818,
            "trip_delay": -499
        }]
    input = json.loads(input)['response']
    actual = download_data.convert_request_to_trip_update(input)
    assert len(actual) == len(expected)

    for i in range(len(actual)):
        expected_item = trip_update.TripUpdate(**expected[i])
        print(expected_item)
        print(actual[i])
        print(expected_item == actual[i])
        assert expected_item == actual[i]


if __name__ == "__main__":
    test_convert_request_to_trip_update()

