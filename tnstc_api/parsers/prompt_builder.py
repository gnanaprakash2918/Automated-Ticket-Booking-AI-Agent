import json
import textwrap
from typing import List, Type
from pydantic import BaseModel
from utils.helpers import minify_html


class PromptGenerator:
    def build_system_prompt(self, pydantic_model: Type[BaseModel]) -> str:
        json_schema = pydantic_model.model_json_schema()

        system_content = textwrap.dedent(f"""
        You are a JSON extraction engine. Output exactly one JSON object that conforms to the provided JSON Schema.
        Do not include conversational text or markdown outside the final JSON.
                                         
        ## JSON Output Schema (Strict Constraint)
        {json.dumps(json_schema, indent=2)}
        """).strip()
        return system_content

    def _build_few_shot_examples(self) -> str:
        examples = self._get_raw_examples()
        parts = ["\n## Few-Shot Examples\n"]
        for idx, example in enumerate(examples, 1):
            main_clean = minify_html(example["main_html"])
            detail_clean = minify_html(example["detail_html"])

            parts.append(
                textwrap.dedent(f"""
            ---
            EXAMPLE {idx}
            ---
            MAIN_LIST_HTML
            {main_clean}
            ---
            DETAIL_TABLE_HTML
            {detail_clean}
            ---
            CORRECT RATIONALE AND JSON OUTPUT
            {json.dumps(example["json_output"], indent=2)}
            ---
            END EXAMPLE {idx}
            ---
            """)
            )
        return "".join(parts)

    def _get_raw_examples(self) -> List[dict]:
        # example 1
        main_html_example_1 = '<html><body><div class="bus-list" data-bus-type="DELUXE 3X2" data-time="00:05"><div class="bus-item"><div class="py-4 px-3"><div class="col"><span class="operator-name">MADURAI</span><span>DELUXE 3X2</span><span><a data-target="#TripcodePopUp" data-toggle="modal"> 0005SALMADMM01L</a> / 104N1</span></div><div class="col time-info"><span>00:05</span><small>SALEM</small></div><div class="col time-info"><span class="duration">6.10Hrs </span><small>Via-KARUR , DINDIGUL</small></div><div class="col time-info"><span>06:15</span><small>MADURAI</small></div><div><div class="price">Rs 195 </div><div id="selectButton0"><span>41 Seats Available</span><br/></div></div></div><div class="center seatLayout" id="dvLoadStatusTR0"><div><h1><span> Please wait ... Loading Seat Layout </span></h1></div></div></div></div></body></html>'

        detail_html_example_1 = '<html><body><table><tbody><tr><td><table><tr><td></td></tr><tr><td><table><tr><td></td><td><table><tr><td></td><td></td><td><div class="boxheader"><span><h2 class="boxheader">Service Details</h2></span></div></td><td></td><td></td></tr></table></td><td></td></tr><tr><td></td><td><table><tr><td><table><tr><td></td></tr><tr><td><div>Service Code :</div></td><td><div>0010SALMADMM01L</div></td><td><div>Route No. :</div></td><td><div>104UB1</div></td></tr><tr><td><div>From Place :</div></td><td><div>SALEM</div></td><td><div>To Place :</div></td><td><div>MADURAI</div></td></tr><tr><td><div>Journey Date:</div></td><td><div>20/11/2025</div></td><td><div>Journey Hours *:</div></td><td><div>5:30</div></td></tr><tr><td><div>Total Kms *:</div></td><td><div>250.00</div></td><td><div>Corporation :</div></td><td><div>MADURAI</div></td></tr><tr><td></td></tr><tr><td><table><tr><td><table id="table5"><tr class="tablecolors"><td></td><td></td><td><div>Adult Fare **</div></td><td><div><span class="button">195</span></div></td><td><div>Child Fare **</div></td><td><div><span class="button">NA</span></div></td></tr></table></td></tr></table></td></tr><tr><td><div class="kv-list"><div class="kv"><span class="k">Sl. No</span><span class="v">City</span></div><div class="kv"><span class="k">1</span><span class="v">SALEM</span></div><div class="kv"><span class="k">2</span><span class="v">MADURAI</span></div></div></td></tr><tr><td class="lable"><div>*Distance and Journey hours are approximate.</div></td></tr><tr><td class="lable"><div>**Concessions and Levies are applicable as per rules.</div></td></tr><tr><td></td></tr><tr><td></td></tr></table></td></tr></table></td><td></td></tr><tr><td></td><td><div><table><tr><td></td><td></td><td><div><a class="dboxheader"></a></div></td><td></td><td></td></tr></table></div></td><td></td></tr></table></td></tr></table></td></tr></tbody></table></body></html>'

        thought_example_1 = "1. Primary Identifiers: `trip_code` ('0005SALMADMM01L') and `route_code` ('104N1') extracted from Main List <a> tag (Source of Truth), ignoring Detail Table mismatch. 2. Dynamic Data: Time, Duration, Price, and Seats extracted from Main List. 3. Secondary: `total_kms` ('250.00') and `child_fare` ('NA') extracted from Detail Table."

        json_output_example_1 = {
            "explanation": thought_example_1,
            "operator": "MADURAI",
            "bus_type": "DELUXE 3X2",
            "trip_code": "0005SALMADMM01L",
            "route_code": "104N1",
            "departure_time": "00:05",
            "arrival_time": "06:15",
            "duration": "6.10",
            "price_in_rs": 195,
            "seats_available": 41,
            "via_route": ["KARUR", "DINDIGUL"],
            "total_kms": "253.00",
            "child_fare": "NA",
        }

        # example 2
        main_html_example_2 = '<html><body><div class="bus-list" data-bus-type="DELUXE 3X2" data-time="00:10"><div class="bus-item"><div class="py-4 px-3"><div class="col"><span class="operator-name">MADURAI</span><span>DELUXE 3X2</span><span><a data-target="#TripcodePopUp" data-toggle="modal"> 0010SALMADMM01L</a> / 104UB1</span></div><div class="col time-info"><span>00:10</span><small>SALEM</small></div><div class="col time-info"><span class="duration">5.30Hrs </span><small>Via-KARUR , DINDIGUL</small></div><div class="col time-info"><span>06:20</span><small>MADURAI</small></div><div><div class="price">Rs 195 </div><div id="selectButton1"><span>41 Seats Available</span><br/></div></div></div><div class="center seatLayout" id="dvLoadStatusTR1"><div><h1><span> Please wait ... Loading Seat Layout </span></h1></div></div></div></div></body></html>'
        detail_html_example_2 = '<html><body><table><tbody><tr><td><table><tr><td></td></tr><tr><td><table><tr><td></td><td><table><tr><td></td><td></td><td><div class="boxheader"><span><h2 class="boxheader">Service Details</h2></span></div></td><td></td><td></td></tr></table></td><td></td></tr><tr><td></td><td><table><tr><td><table><tr><td></td></tr><tr><td><div>Service Code :</div></td><td><div>0010SALMADMM01L</div></td><td><div>Route No. :</div></td><td><div>104UB1</div></td></tr><tr><td><div>From Place :</div></td><td><div>SALEM</div></td><td><div>To Place :</div></td><td><div>MADURAI</div></td></tr><tr><td><div>Journey Date:</div></td><td><div>20/11/2025</div></td><td><div>Journey Hours *:</div></td><td><div>5:30</div></td></tr><tr><td><div>Total Kms *:</div></td><td><div>250.00</div></td><td><div>Corporation :</div></td><td><div>MADURAI</div></td></tr><tr><td></td></tr><tr><td><table><tr><td><table id="table5"><tr class="tablecolors"><td></td><td></td><td><div>Adult Fare **</div></td><td><div><span class="button">195</span></div></td><td><div>Child Fare **</div></td><td><div><span class="button">NA</span></div></td></tr></table></td></tr></table></td></tr><tr><td><div class="kv-list"><div class="kv"><span class="k">Sl. No</span><span class="v">City</span></div><div class="kv"><span class="k">1</span><span class="v">SALEM</span></div><div class="kv"><span class="k">2</span><span class="v">MADURAI</span></div></div></td></tr><tr><td class="lable"><div>*Distance and Journey hours are approximate.</div></td></tr><tr><td class="lable"><div>**Concessions and Levies are applicable as per rules.</div></td></tr><tr><td></td></tr><tr><td></td></tr></table></td></tr></table></td><td></td></tr><tr><td></td><td><div><table><tr><td></td><td></td><td><div><a class="dboxheader"></a></div></td><td></td><td></td></tr></table></div></td><td></td></tr></table></td></tr></table></td></tr></tbody></table></body></html>'

        thought_example_2 = "1. Primary Identifiers: `trip_code` ('0010SALMADMM01L') and `route_code` ('104UB1') extracted from Main List (Source of Truth). 2. Dynamic Data: `seats_available` (41), Price (195), and Times extracted from Main List. 3. Secondary: `total_kms` ('250.00') extracted from Detail Table."

        json_output_example_2 = {
            "explanation": thought_example_2,
            "operator": "MADURAI",
            "bus_type": "DELUXE 3X2",
            "trip_code": "0010SALMADMM01L",
            "route_code": "104UB1",
            "departure_time": "00:10",
            "arrival_time": "06:20",
            "duration": "5.30",
            "price_in_rs": 195,
            "seats_available": 41,
            "via_route": ["KARUR", "DINDIGUL"],
            "total_kms": "250.00",
            "child_fare": "NA",
        }

        # example 3
        main_html_example_3 = '<html><body><div class="bus-list" data-bus-type="AC 3X2" data-time="00:30"><div class="bus-item"><div class="py-4 px-3"><div class="col"><span class="operator-name">SALEM</span><span>AC 3X2</span><span><a data-target="#TripcodePopUp" data-toggle="modal"> 0030SALBANDD02A</a> / 100J</span></div><div class="col time-info"><span>00:30</span><small>SALEM</small></div><div class="col time-info"><span class="duration">5.00Hrs </span><small>Via-HOSUR</small></div><div class="col time-info"><span>05:30</span><small>BENGALURU</small></div><div><div class="price">Rs 269 </div><div id="selectButton0"><span>46 Seats Available</span><br/></div></div></div><div class="center seatLayout" id="dvLoadStatusTR0"><div><h1><span> Please wait ... Loading Seat Layout </span></h1></div></div></div></div></body></html>'
        detail_html_example_3 = '<html><body><table><tbody><tr><td><table><tr><td></td></tr><tr><td><table><tr><td></td><td><table><tr><td></td><td></td><td><div class="boxheader"><span><h2 class="boxheader">Service Details</h2></span></div></td><td></td><td></td></tr></table></td><td></td></tr><tr><td></td><td><table><tr><td><table><tr><td></td></tr><tr><td><div>Service Code :</div></td><td><div>0030SALBANDD02A</div></td><td><div>Route No. :</div></td><td><div>100J</div></td></tr><tr><td><div>From Place :</div></td><td><div>SALEM</div></td><td><div>To Place :</div></td><td><div>BENGALURU</div></td></tr><tr><td><div>Journey Date:</div></td><td><div>20/11/2025</div></td><td><div>Journey Hours *:</div></td><td><div>5:00</div></td></tr><tr><td><div>Total Kms *:</div></td><td><div>208.00</div></td><td><div>Corporation :</div></td><td><div>SALEM</div></td></tr><tr><td></td></tr><tr><td><table><tr><td><table id="table5"><tr class="tablecolors"><td></td><td></td><td><div>Adult Fare **</div></td><td><div><span class="button">269</span></div></td><td><div>Child Fare **</div></td><td><div><span class="button">NA</span></div></td></tr></table></td></tr></table></td></tr><tr><td><div class="kv-list"><div class="kv"><span class="k">Sl. No</span><span class="v">City</span></div><div class="kv"><span class="k">1</span><span class="v">SALEM</span></div><div class="kv"><span class="k">2</span><span class="v">BENGALURU</span></div></div></td></tr><tr><td class="lable"><div>*Distance and Journey hours are approximate.</div></td></tr><tr><td class="lable"><div>**Concessions and Levies are applicable as per rules.</div></td></tr><tr><td></td></tr><tr><td></td></tr></table></td></tr></table></td><td></td></tr><tr><td></td><td><div><table><tr><td></td><td></td><td><div><a class="dboxheader"></a></div></td><td></td><td></td></tr></table></div></td><td></td></tr></table></td></tr></table></td></tr></tbody></table></body></html>'

        thought_example_3 = "1. Primary Identifiers: `trip_code` ('0030SALBANDD02A') and `route_code` ('100J') extracted from Main List (Source of Truth). 2. Dynamic Data: `seats_available` (46) and Price (269) extracted from Main List. 3. Secondary: `total_kms` ('208.00') extracted from Detail Table."

        json_output_example_3 = {
            "explanation": thought_example_3,
            "operator": "SALEM",
            "bus_type": "AC 3X2",
            "trip_code": "0030SALBANDD02A",
            "route_code": "100J",
            "departure_time": "00:30",
            "arrival_time": "05:30",
            "duration": "5.00",
            "price_in_rs": 269,
            "seats_available": 46,
            "via_route": ["HOSUR"],
            "total_kms": "208.00",
            "child_fare": "NA",
        }

        # example 4
        main_html_example_4 = '<html><body><div class="bus-list"><span class="operator-name">AIRAVAT</span><span><a> 2345AIRBANGD01A</a> / S78A</span><div class="price">Rs 750 </div></div></body></html>'

        detail_html_example_4 = "<html><body><h1>Server Error</h1></body></html>"

        thought_example_4 = "1. Detail HTML is broken (Server Error). 2. Applied Fallback Rule: Extracted `trip_code`, `route_code`, `operator`, and `price_in_rs` directly from Main List. 3. All missing fields set to 'NA' or null."

        json_output_example_4 = {
            "explanation": thought_example_4,
            "operator": "AIRAVAT",
            "bus_type": "NA",
            "trip_code": "2345AIRBANGD01A",
            "route_code": "S78A",
            "departure_time": "NA",
            "arrival_time": "NA",
            "duration": "NA",
            "price_in_rs": 750,
            "seats_available": "NA",
            "via_route": None,
            "total_kms": "NA",
            "child_fare": "NA",
        }

        examples = [
            {
                "main": main_html_example_1,
                "detail": detail_html_example_1,
                "json": json_output_example_1,
            },
            {
                "main": main_html_example_2,
                "detail": detail_html_example_2,
                "json": json_output_example_2,
            },
            {
                "main": main_html_example_3,
                "detail": detail_html_example_3,
                "json": json_output_example_3,
            },
            {
                "main": main_html_example_4,
                "detail": detail_html_example_4,
                "json": json_output_example_4,
            },
        ]

        return [
            {
                "main_html": item["main"],
                "detail_html": item["detail"],
                "json_output": item["json"],
            }
            for item in examples
        ]
