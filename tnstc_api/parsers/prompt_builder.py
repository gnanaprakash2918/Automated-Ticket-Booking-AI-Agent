import json
import inspect
import textwrap
from typing import Any, get_args, get_origin, Union, Annotated, List, Type
from pydantic import BaseModel
from utils.helpers import minify_html

def _get_base_type(type_hint: Any) -> Any:
    origin = get_origin(type_hint)
    args = get_args(type_hint)
    if origin is Union:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return _get_base_type(non_none[0])
        return type_hint
    if origin is Annotated:
        return _get_base_type(args[0])
    if origin in (list, List, tuple):
        return _get_base_type(args[0]) if args else type_hint
    if args:
        return _get_base_type(args[0])
    return type_hint

def extract_examples(model: Type[BaseModel], visited: set = None) -> str:
    if visited is None:
        visited = set()
    if model in visited:
        return ""
    visited.add(model)
    example_str = ""
    model_config = getattr(model, "model_config", {})
    if isinstance(model_config, dict) and "json_schema_extra" in model_config:
        extra = model_config["json_schema_extra"]
        if isinstance(extra, dict) and "examples" in extra:
            examples = extra["examples"]
            if examples:
                example_str += f"- **{model.__name__} Examples:**\n"
                for i, example in enumerate(examples):
                    example_str += f"  - **Example {i + 1}:**\n"
                    example_str += "```json\n" + json.dumps(example, indent=2) + "\n```\n"
    for name, field in model.model_fields.items():
        field_type = _get_base_type(field.annotation)
        if inspect.isclass(field_type) and issubclass(field_type, BaseModel) and field_type != model:
            nested = extract_examples(field_type, visited)
            if nested:
                if example_str:
                    example_str += "\n\n"
                example_str += nested
    return example_str.strip()

class PromptGenerator:
    def build_system_prompt(self, pydantic_model: Type[BaseModel]) -> str:
        json_schema = pydantic_model.model_json_schema()
        examples_hint = extract_examples(pydantic_model)
        system_content = textwrap.dedent(f"""
        You are a JSON extraction engine. Output exactly one JSON object that conforms to the provided JSON Schema.
        Do not include conversational text or markdown outside the final JSON.
        ## Reference Examples for Data Formatting
        {examples_hint}
        ## JSON Output Schema (Strict Constraint)
        {json.dumps(json_schema, indent=2)}
        """).strip()
        return system_content

    def build_cot_user_prompt(self, main_html: str, detail_html: str) -> str:
        cot_instruction = textwrap.dedent("""
        ## Extraction Instruction
        1. Analyze MAIN_LIST_HTML and DETAIL_TABLE_HTML.
        2. Extract all required fields per the system JSON Schema.
        3. Provide a concise, high-level mapping rationale (no internal chain-of-thought) and place it under the top-level key "explanation". Keep the explanation to at most 3 short numbered steps.
        4. Output a single valid JSON object containing the extracted fields and the "explanation" key only.
        """).strip()
        few_shot_examples = self._build_few_shot_examples()
        new_data_section = textwrap.dedent(f"""
        ---
        NEW DATA TO PROCESS
        ---
        MAIN_LIST_HTML
        {minify_html(main_html)}
        ---
        DETAIL_TABLE_HTML
        {minify_html(detail_html)}
        ---
        FINAL JSON OUTPUT
        """)
        return cot_instruction + "\n\n" + few_shot_examples + "\n" + new_data_section

    def _build_few_shot_examples(self) -> str:
        examples = self._get_raw_examples()
        parts = ["\n## Few-Shot Examples\n"]
        for idx, example in enumerate(examples, 1):
            parts.append(textwrap.dedent(f"""
            ---
            EXAMPLE {idx}
            ---
            MAIN_LIST_HTML
            {minify_html(example["main_html"])}
            ---
            DETAIL_TABLE_HTML
            {minify_html(example["detail_html"])}
            ---
            CORRECT RATIONALE AND JSON OUTPUT
            {json.dumps(example["json_output"], indent=2)}
            ---
            END EXAMPLE {idx}
            ---
            """))
        return "".join(parts)

    def _get_raw_examples(self) -> List[dict]:
        main_html_0 = '<html><body><div class="bus-list" data-bus-type="AC 3X2" data-time="00:30"><div class="bus-item"><div class="py-4 px-3"><div class="col"><span class="operator-name">SALEM</span><span>AC 3X2</span><span><a data-target="#TripcodePopUp" data-toggle="modal"> 0030SALBANDD02A</a> / 100J</span></div><div class="col time-info"><span>00:30</span><small>SALEM</small></div><div class="col time-info"><span class="duration">5.00Hrs </span><small>Via-HOSUR</small></div><div class="col time-info"><span>05:30</span><small>BENGALURU</small></div><div><div class="price">Rs 269 </div><div id="selectButton0"><span>42 Seats Available</span><br/></div></div></div><div class="center seatLayout" id="dvLoadStatusTR0"><div><h1><span> Please wait ... Loading Seat Layout </span></h1></div></div></div></div></body></html>'
        
        detail_html_0 = '<html><body><table><tbody><tr><td><table><tr><td></td></tr><tr><td><table><tr><td></td><td><table><tr><td></td><td></td><td><div class="boxheader"><span><h2 class="boxheader">Service Details</h2></span></div></td><td></td><td></td></tr></table></td><td></td></tr><tr><td></td><td><table><tr><td><table><tr><td></td></tr><tr><td><div>Service Code :</div></td><td><div>0030SALBANDD02A</div></td><td><div>Route No. :</div></td><td><div>100J</div></td></tr><tr><td><div>From Place :</div></td><td><div>SALEM</div></td><td><div>To Place :</div></td><td><div>BENGALURU</div></td></tr><tr><td><div>Journey Date:</div></td><td><div>18/11/2025</div></td><td><div>Journey Hours *:</div></td><td><div>5:00</div></td></tr><tr><td><div>Total Kms *:</div></td><td><div>208.00</div></td><td><div>Corporation :</div></td><td><div>SALEM</div></td></tr><tr><td></td></tr><tr><td><table><tr><td><table id="table5"><tr class="tablecolors"><td></td><td></td><td><div>Adult Fare **</div></td><td><div><span class="button">269</span></div></td><td><div>Child Fare **</div></td><td><div><span class="button">NA</span></div></td></tr></table></td></tr></table></td></tr><tr><td><div class="kv-list"><div class="kv"><span class="k">Sl. No</span><span class="v">City</span></div><div class="kv"><span class="k">1</span><span class="v">SALEM</span></div><div class="kv"><span class="k">2</span><span class="v">BENGALURU</span></div></div></td></tr><tr><td class="lable"><div>*Distance and Journey hours are approximate.</div></td></tr><tr><td class="lable"><div>**Concessions and Levies are applicable as per rules.</div></td></tr><tr><td></td></tr><tr><td></td></tr></table></td></tr></table></td><td></td></tr><tr><td></td><td><div><table><tr><td></td><td></td><td><div><a class="dboxheader"></a></div></td><td></td><td></td></tr></table></div></td><td></td></tr></table></td></tr></table></td></tr></tbody></table></body></html>'
        
        thought_0 = textwrap.dedent("""
        Step 1: Extracted primary details from MAIN_LIST_HTML. Captured the **operator** ('SALEM') from the `.operator-name` span and the **bus_type** ('AC 3X2') from the parent div's `data-bus-type` attribute.
        Step 2: Extracted time and route information. The **departure_time** is '00:30', **arrival_time** is '05:30', and **duration** '5.00' was parsed from '5.00Hrs'. Identified **via_route** as ['HOSUR'].
        Step 3: Extracted financial data. The **price_in_rs** is 269 from the `.price` div, and **seats_available** is 42.
        Step 4: Extracted codes from the anchor tag and confirmed with DETAIL_TABLE_HTML. The **trip_code** ('0030SALBANDD02A') and **route_code** ('100J') are present in both the main list anchor text and the detail table's 'Service Code' and 'Route No.'.
        Step 5: Extracted secondary details from DETAIL_TABLE_HTML. The **total_kms** '208.00' was extracted from the 'Total Kms' key-value pair, and the **child_fare** was confirmed as 'NA'.
        """).strip()

        json_output_0 = {
            "explanation": thought_0,
            "operator": "SALEM",
            "bus_type": "AC 3X2",
            "trip_code": "0030SALBANDD02A",
            "route_code": "100J",
            "departure_time": "00:30",
            "arrival_time": "05:30",
            "duration": "5.00",
            "price_in_rs": 269,
            "seats_available": 42,
            "via_route": [
                "HOSUR"
            ],
            "total_kms": "208.00",
            "child_fare": "NA"
        }

        main_html_1 = '<html><body><div class="bus-list" data-bus-type="DELUXE 3X2" data-time="00:05"><div class="bus-item"><div class="py-4 px-3"><div class="col"><span class="operator-name">MADURAI</span><span>DELUXE 3X2</span><span><a data-target="#TripcodePopUp" data-toggle="modal"> 0005SALMADMM01L</a> / 104N1</span></div><div class="col time-info"><span>00:05</span><small>SALEM</small></div><div class="col time-info"><span class="duration">6.10Hrs </span><small>Via-KARUR , DINDIGUL</small></div><div class="col time-info"><span>06:15</span><small>MADURAI</small></div><div><div class="price">Rs 195 </div><div id="selectButton0"><span>43 Seats Available</span><br/></div></div></div><div class="center seatLayout" id="dvLoadStatusTR0"><div><h1><span> Please wait ... Loading Seat Layout </span></h1></div></div></div></div></body></html>'
        
        detail_html_1 = '<html><body><table><tbody><tr><td><table><tr><td></td></tr><tr><td><table><tr><td></td><td><table><tr><td></td><td></td><td><div class="boxheader"><span><h2 class="boxheader">Service Details</h2></span></div></td><td></td><td></td></tr></table></td><td></td></tr><tr><td></td><td><table><tr><td><table><tr><td></td></tr><tr><td><div>Service Code :</div></td><td><div>0010SALMADMM01L</div></td><td><div>Route No. :</div></td><td><div>104UB1</div></td></tr><tr><td><div>From Place :</div></td><td><div>SALEM</div></td><td><div>To Place :</div></td><td><div>MADURAI</div></td></tr><tr><td><div>Journey Date:</div></td><td><div>18/11/2025</div></td><td><div>Journey Hours *:</div></td><td><div>5:30</div></td></tr><tr><td><div>Total Kms *:</div></td><td><div>250.00</div></td><td><div>Corporation :</div></td><td><div>MADURAI</div></td></tr><tr><td></td></tr><tr><td><table><tr><td><table id="table5"><tr class="tablecolors"><td></td><td></td><td><div>Adult Fare **</div></td><td><div><span class="button">195</span></div></td><td><div>Child Fare **</div></td><td><div><span class="button">NA</span></div></td></tr></table></td></tr></table></td></tr><tr><td><div class="kv-list"><div class="kv"><span class="k">Sl. No</span><span class="v">City</span></div><div class="kv"><span class="k">1</span><span class="v">SALEM</span></div><div class="kv"><span class="k">2</span><span class="v">MADURAI</span></div></div></td></tr><tr><td class="lable"><div>*Distance and Journey hours are approximate.</div></td></tr><tr><td class="lable"><div>**Concessions and Levies are applicable as per rules.</div></td></tr><tr><td></td></tr><tr><td></td></tr></table></td></tr></table></td><td></td></tr><tr><td></td><td><div><table><tr><td></td><td></td><td><div><a class="dboxheader"></a></div></td><td></td><td></td></tr></table></div></td><td></td></tr></table></td></tr></table></td></tr></tbody></table></body></html>'
        
        thought_1 = textwrap.dedent("""
        Step 1: Extracted primary details from MAIN_LIST_HTML. Captured the **operator** ('MADURAI') and **bus_type** ('DELUXE 3X2').
        Step 2: Extracted time and route information. Found **departure_time** ('00:05'), **arrival_time** ('06:15'), and parsed **duration** ('6.10'). Determined **via_route** list as ['KARUR', 'DINDIGUL'] by splitting the comma-separated text.
        Step 3: Extracted financial data and codes from MAIN_LIST_HTML. The **price_in_rs** is 195, and **seats_available** is 43. The visible route code is '104N1'.
        Step 4: Cross-referenced with DETAIL_TABLE_HTML. Used the canonical **trip_code** ('0010SALMADMM01L') from the 'Service Code' in the detail table. Confirmed the **route_code** as '104N1' (from main list) over '104UB1' (from detail list, prioritizing the main list one as it is directly associated with the service entry).
        Step 5: Extracted secondary details from DETAIL_TABLE_HTML, capturing **total_kms** ('250.00') and confirming **child_fare** ('NA').
        """).strip()
        
        json_output_1 = {
            "explanation": thought_1,
            "operator": "MADURAI",
            "bus_type": "DELUXE 3X2",
            "trip_code": "0010SALMADMM01L",
            "route_code": "104N1",
            "departure_time": "00:05",
            "arrival_time": "06:15",
            "duration": "6.10",
            "price_in_rs": 195,
            "seats_available": 43,
            "via_route": [
                "KARUR",
                "DINDIGUL"
            ],
            "total_kms": "250.00",
            "child_fare": "NA"
        }

        main_html_2 = '<html><body><div class="bus-list" data-bus-type="DELUXE 3X2" data-time="00:10"><div class="bus-item"><div class="py-4 px-3"><div class="col"><span class="operator-name">MADURAI</span><span>DELUXE 3X2</span><span><a data-target="#TripcodePopUp" data-toggle="modal"> 0010SALMADMM01L</a> / 104UB1</span></div><div class="col time-info"><span>00:10</span><small>SALEM</small></div><div class="col time-info"><span class="duration">5.30Hrs </span><small>Via-KARUR , DINDIGUL</small></div><div class="col time-info"><span>06:20</span><small>MADURAI</small></div><div><div class="price">Rs 195 </div><div id="selectButton1"><span>43 Seats Available</span><br/></div></div></div><div class="center seatLayout" id="dvLoadStatusTR1"><div><h1><span> Please wait ... Loading Seat Layout </span></h1></div></div></div></div></body></html>'
        
        detail_html_2 = '<html><body><table><tbody><tr><td><table><tr><td></td></tr><tr><td><table><tr><td></td><td><table><tr><td></td><td></td><td><div class="boxheader"><span><h2 class="boxheader">Service Details</h2></span></div></td><td></td><td></td></tr></table></td><td></td></tr><tr><td></td><td><table><tr><td><table><tr><td></td></tr><tr><td><div>Service Code :</div></td><td><div>0010SALMADMM01L</div></td><td><div>Route No. :</div></td><td><div>104UB1</div></td></tr><tr><td><div>From Place :</div></td><td><div>SALEM</div></td><td><div>To Place :</div></td><td><div>MADURAI</div></td></tr><tr><td><div>Journey Date:</div></td><td><div>18/11/2025</div></td><td><div>Journey Hours *:</div></td><td><div>5:30</div></td></tr><tr><td><div>Total Kms *:</div></td><td><div>250.00</div></td><td><div>Corporation :</div></td><td><div>MADURAI</div></td></tr><tr><td></td></tr><tr><td><table><tr><td><table id="table5"><tr class="tablecolors"><td></td><td></td><td><div>Adult Fare **</div></td><td><div><span class="button">195</span></div></td><td><div>Child Fare **</div></td><td><div><span class="button">NA</span></div></td></tr></table></td></tr></table></td></tr><tr><td><div class="kv-list"><div class="kv"><span class="k">Sl. No</span><span class="v">City</span></div><div class="kv"><span class="k">1</span><span class="v">SALEM</span></div><div class="kv"><span class="k">2</span><span class="v">MADURAI</span></div></div></td></tr><tr><td class="lable"><div>*Distance and Journey hours are approximate.</div></td></tr><tr><td class="lable"><div>**Concessions and Levies are applicable as per rules.</div></td></tr><tr><td></td></tr><tr><td></td></tr></table></td></tr></table></td><td></td></tr><tr><td></td><td><div><table><tr><td></td><td></td><td><div><a class="dboxheader"></a></div></td><td></td><td></tr></table></div></td><td></td></tr></table></td></tr></table></td></tr></tbody></table></body></html>'
        
        thought_2 = textwrap.dedent("""
        Step 1: Extracted primary details from MAIN_LIST_HTML. Captured the **operator** ('MADURAI') and **bus_type** ('DELUXE 3X2').
        Step 2: Extracted time and route information. Found **departure_time** ('00:10'), **arrival_time** ('06:20'), and parsed **duration** ('5.30'). Determined **via_route** list as ['KARUR', 'DINDIGUL'].
        Step 3: Extracted financial data and codes from MAIN_LIST_HTML. The **price_in_rs** is 195, and **seats_available** is 43.
        Step 4: Cross-referenced and finalized codes with DETAIL_TABLE_HTML. Both **trip_code** ('0010SALMADMM01L') and **route_code** ('104UB1') were extracted from the detail table (Service Code/Route No.).
        Step 5: Extracted secondary details from DETAIL_TABLE_HTML, capturing **total_kms** ('250.00') and confirming **child_fare** ('NA').
        """).strip()
        
        json_output_2 = {
            "explanation": thought_2,
            "operator": "MADURAI",
            "bus_type": "DELUXE 3X2",
            "trip_code": "0010SALMADMM01L",
            "route_code": "104UB1",
            "departure_time": "00:10",
            "arrival_time": "06:20",
            "duration": "5.30",
            "price_in_rs": 195,
            "seats_available": 43,
            "via_route": [
                "KARUR",
                "DINDIGUL"
            ],
            "total_kms": "250.00",
            "child_fare": "NA"
        }

    
        main_html_3 = '<html><body><div class="bus-list" data-bus-type="DELUXE 3X2" data-time="00:10"><div class="bus-item"><div class="py-4 px-3"><div class="col"><span class="operator-name">MADURAI</span><span>DELUXE 3X2</span><span><a data-target="#TripcodePopUp" data-toggle="modal"> 0010SALMADMM01L</a> / 104UB1</span></div><div class="col time-info"><span>00:10</span><small>SALEM</small></div><div class="col time-info"><span class="duration">5.30Hrs </span><small>Via-KARUR , DINDIGUL</small></div><div class="col time-info"><span>06:20</span><small>MADURAI</small></div><div><div class="price">Rs 195 </div><div id="selectButton1"><span>43 Seats Available</span><br/></div></div></div><div class="center seatLayout" id="dvLoadStatusTR1"><div><h1><span> Please wait ... Loading Seat Layout </span></h1></div></div></div></div></body></html>'
        
        detail_html_3 = '<html><body><table><tbody><tr><td><table><tr><td></td></tr><tr><td><table><tr><td></td><td><table><tr><td></td><td></td><td><div class="boxheader"><span><h2 class="boxheader">Service Details</h2></span></div></td><td></td><td></td></tr></table></td><td></td></tr><tr><td></td><td><table><tr><td><table><tr><td></td></tr><tr><td><div>Service Code :</div></td><td><div>0010SALMADMM01L</div></td><td><div>Route No. :</div></td><td><div>104UB1</div></td></tr><tr><td><div>From Place :</div></td><td><div>SALEM</div></td><td><div>To Place :</div></td><td><div>MADURAI</div></td></tr><tr><td><div>Journey Date:</div></td><td><div>18/11/2025</div></td><td><div>Journey Hours *:</div></td><td><div>5:30</div></td></tr><tr><td><div>Total Kms *:</div></td><td><div>250.00</div></td><td><div>Corporation :</div></td><td><div>MADURAI</div></td></tr><tr><td></td></tr><tr><td><table><tr><td><table id="table5"><tr class="tablecolors"><td></td><td></td><td><div>Adult Fare **</div></td><td><div><span class="button">195</span></div></td><td><div>Child Fare **</div></td><td><div><span class="button">NA</span></div></td></tr></table></td></tr></table></td></tr><tr><td><div class="kv-list"><div class="kv"><span class="k">Sl. No</span><span class="v">City</span></div><div class="kv"><span class="k">1</span><span class="v">SALEM</span></div><div class="kv"><span class="k">2</span><span class="v">MADURAI</span></div></div></td></tr><tr><td class="lable"><div>*Distance and Journey hours are approximate.</div></td></tr><tr><td class="lable"><div>**Concessions and Levies are applicable as per rules.</div></td></tr><tr><td></td></tr><tr><td></td></tr></table></td></tr></table></td><td></td></tr><tr><td></td><td><div><table><tr><td></td><td></td><td><div><a class="dboxheader"></a></div></td><td></td><td></td></tr></table></div></td><td></td></tr></table></td></tr></table></td></tr></tbody></table></body></html>'
        
        explanation_3 = "Details match between main list and detail table. Times, route, price, and codes are consistent."
        
        json_output_3 = {
            "explanation": explanation_3,
            "operator": "MADURAI",
            "bus_type": "DELUXE 3X2",
            "trip_code": "0010SALMADMM01L",
            "route_code": "104UB1",
            "departure_time": "00:10",
            "arrival_time": "06:20",
            "duration": "5.30",
            "price_in_rs": 195,
            "seats_available": 43,
            "via_route": ["KARUR", "DINDIGUL"],
            "total_kms": "250.00",
            "child_fare": "NA"
        }

        main_html_4 = '<html><body><div class="bus-list" data-bus-type="DELUXE 3X2" data-time="00:10"><div class="bus-item"><div class="py-4 px-3"><div class="col"><span class="operator-name">MADURAI</span><span>DELUXE 3X2</span><span><a data-target="#TripcodePopUp" data-toggle="modal"> 0010SALMADMM01L</a> / 104UB1</span></div><div class="col time-info"><span>00:10</span><small>SALEM</small></div><div class="col time-info"><span class="duration">5.30Hrs </span><small>Via-KARUR , DINDIGUL</small></div><div class="col time-info"><span>06:20</span><small>MADURAI</small></div><div><div class="price">Rs 195 </div><div id="selectButton1"><span>43 Seats Available</span><br/></div></div></div><div class="center seatLayout" id="dvLoadStatusTR1"><div><h1><span> Please wait ... Loading Seat Layout </span></h1></div></div></div></div></body></html>'
        
        detail_html_4 = '<html><body><table><tbody><tr><td><table><tr><td></td></tr><tr><td><table><tr><td></td><td><table><tr><td></td><td></td><td><div class="boxheader"><span><h2 class="boxheader">Service Details</h2></span></div></td><td></td><td></td></tr></table></td><td></td></tr><tr><td></td><td><table><tr><td><table><tr><td></td></tr><tr><td><div>Service Code :</div></td><td><div>0055SALMADMM05L</div></td><td><div>Route No. :</div></td><td><div>006G</div></td></tr><tr><td><div>From Place :</div></td><td><div>SALEM</div></td><td><div>To Place :</div></td><td><div>MADURAI</div></td></tr><tr><td><div>Journey Date:</div></td><td><div>18/11/2025</div></td><td><div>Journey Hours *:</div></td><td><div>6:12</div></td></tr><tr><td><div>Total Kms *:</div></td><td><div>283.00</div></td><td><div>Corporation :</div></td><td><div>MADURAI</div></td></tr><tr><td></td></tr><tr><td><table><tr><td><table id="table5"><tr class="tablecolors"><td></td><td></td><td><div>Adult Fare **</div></td><td><div><span class="button">185</span></div></td><td><div>Child Fare **</div></td><td><div><span class="button">NA</span></div></td></tr></table></td></tr></table></td></tr><tr><td><div class="kv-list"><div class="kv"><span class="k">Sl. No</span><span class="v">City</span></div><div class="kv"><span class="k">1</span><span class="v">SALEM</span></div><div class="kv"><span class="k">2</span><span class="v">MADURAI</span></div></div></td></tr><tr><td class="lable"><div>*Distance and Journey hours are approximate.</div></td></tr><tr><td class="lable"><div>**Concessions and Levies are applicable as per rules.</div></td></tr><tr><td></td></tr><tr><td></td></tr></table></td></tr></table></td><td></td></tr><tr><td></td><td><div><table><tr><td></td><td></td><td><div><a class="dboxheader"></a></div></td><td></td><td></td></tr></table></div></td><td></td></tr></table></td></tr></table></td></tr></tbody></table></body></html>'
        
        explanation_4 = "Detail table provides the correct trip code, route code, kms, and price; main list provides times and via route."
        
        json_output_4 = {
            "explanation": explanation_4,
            "operator": "MADURAI",
            "bus_type": "DELUXE 3X2",
            "trip_code": "0055SALMADMM05L",
            "route_code": "006G",
            "departure_time": "00:10",
            "arrival_time": "06:20",
            "duration": "5.30",
            "price_in_rs": 185,
            "seats_available": 43,
            "via_route": ["KARUR", "DINDIGUL"],
            "total_kms": "283.00",
            "child_fare": "NA"
        }

        main_html_5 = '<html><body><div class="bus-list" data-bus-type="DELUXE 3X2" data-time="00:15"><div class="bus-item"><div class="py-4 px-3"><div class="col"><span class="operator-name">MADURAI</span><span>DELUXE 3X2</span><span><a data-target="#TripcodePopUp" data-toggle="modal"> 0015SALMADMM01L</a> / 104B2</span></div><div class="col time-info"><span>00:15</span><small>SALEM</small></div><div class="col time-info"><span class="duration">5.30Hrs </span><small>Via-KARUR,DINDIGUL</small></div><div class="col time-info"><span>06:25</span><small>MADURAI</small></div><div><div class="price">Rs 195 </div><div id="selectButton2"><span>43 Seats Available</span><br/></div></div></div><div class="center seatLayout" id="dvLoadStatusTR2"><div><h1><span> Please wait ... Loading Seat Layout </span></h1></div></div></div></div></body></html>'
        
        detail_html_5 = '<html><body><table><tbody><tr><td><table><tr><td></td></tr><tr><td><table><tr><td></td><td><table><tr><td></td><td></td><td><div class="boxheader"><span><h2 class="boxheader">Service Details</h2></span></div></td><td></td><td></td></tr></table></td><td></td></tr><tr><td></td><td><table><tr><td><table><tr><td></td></tr><tr><td><div>Service Code :</div></td><td><div>0055SALMADMM05L</div></td><td><div>Route No. :</div></td><td><div>006D</div></td></tr><tr><td><div>From Place :</div></td><td><div>SALEM</div></td><td><div>To Place :</div></td><td><div>MADURAI</div></td></tr><tr><td><div>Journey Date:</div></td><td><div>18/11/2025</div></td><td><div>Journey Hours *:</div></td><td><div>6:12</div></td></tr><tr><td><div>Total Kms *:</div></td><td><div>283.00</div></td><td><div>Corporation :</div></td><td><div>MADURAI</div></td></tr><tr><td></td></tr><tr><td><table><tr><td><table id="table5"><tr class="tablecolors"><td></td><td></td><td><div>Adult Fare **</div></td><td><div><span class="button">185</span></div></td><td><div>Child Fare **</div></td><td><div><span class="button">NA</span></div></td></tr></table></td></tr></table></td></tr><tr><td><div class="kv-list"><div class="kv"><span class="k">Sl. No</span><span class="v">City</span></div><div class="kv"><span class="k">1</span><span class="v">SALEM</span></div><div class="kv"><span class="k">2</span><span class="v">MADURAI</span></div></div></td></tr><tr><td class="lable"><div>*Distance and Journey hours are approximate.</div></td></tr><tr><td class="lable"><div>**Concessions and Levies are applicable as per rules.</div></td></tr><tr><td></td></tr><tr><td></td></tr></table></td></tr></table></td><td></td></tr><tr><td></td><td><div><table><tr><td></td><td></td><td><div><a class="dboxheader"></a></div></td><td></td><td></td></tr></table></div></td><td></td></tr></table></td></tr></table></td></tr></tbody></table></body></html>'
        
        explanation_5 = "Detail table overrides main list for trip code, route code, and price; other values come from main list."
        
        json_output_5 = {
            "explanation": explanation_5,
            "operator": "MADURAI",
            "bus_type": "DELUXE 3X2",
            "trip_code": "0055SALMADMM05L",
            "route_code": "006D",
            "departure_time": "00:15",
            "arrival_time": "06:25",
            "duration": "5.30",
            "price_in_rs": 185,
            "seats_available": 43,
            "via_route": ["KARUR", "DINDIGUL"],
            "total_kms": "283.00",
            "child_fare": "NA"
        }

        main_html_6 = '<html><body><div class="bus-list" data-bus-type="AC Sleeper 2X1" data-time="23:45"><div class="bus-item"><div class="py-4 px-3"><div class="col"><span class="operator-name">AIRAVAT</span><span>AC Sleeper 2X1</span><span><a data-target="#TripcodePopUp" data-toggle="modal"> 2345AIRBANGD01A</a> / S78A</span></div><div class="col time-info"><span>23:45</span><small>BENGALURU</small></div><div class="col time-info"><span class="duration"></span><small>Via-TUMKUR</small></div><div class="col time-info"><span>05:15</span><small>DHARWAD</small></div><div><div class="price"></div><div id="selectButton3"><span>Seats: N/A</span><br/></div></div></div><div class="center seatLayout" id="dvLoadStatusTR3"><div><h1><span> Please wait ... Loading Seat Layout </span></h1></div></div></div></div></body></html>'
        
        detail_html_6 = '<html><body><table><tbody><tr><td><table><tr><td></td></tr><tr><td><table><tr><td></td><td><table><tr><td></td><td></td><td><div class="boxheader"><span><h2 class="boxheader">Service Details</h2></span></div></td><td></td><td></td></tr></table></td><td></td></tr><tr><td></td><td><table><tr><td><table><tr><td></td></tr><tr><td><div>Service Code :</div></td><td><div>2345AIRBANGD01A</div></td><td><div>Route No. :</div></td><td><div>S78A</div></td></tr><tr><td><div>From Place :</div></td><td><div>BENGALURU</div></td><td><div>To Place :</div></td><td><div>DHARWAD</div></td></tr><tr><td><div>Journey Date:</div></td><td><div>19/11/2025</div></td><td><div>Journey Hours *:</div></td><td><div>Unknown</div></td></tr><tr><td><div>Total Kms *:</div></td><td><div>420.00</div></td><td><div>Corporation :</div></td><td><div>AIRAVAT</div></td></tr><tr><td></td></tr><tr><td><table><tr><td><table id="table5"><tr class="tablecolors"><td></td><td></td><td><div>Adult Fare **</div></td><td><div><span class="button">750</span></div></td><td><div>Child Fare **</div></td><td><div><span class="button">0</span></div></td></tr></table></td></tr></table></td></tr><tr><td><div class="kv-list"><div class="kv"><span class="k">Sl. No</span><span class="v">City</span></div><div class="kv"><span class="k">1</span><span class="v">BENGALURU</span></div><div class="kv"><span class="k">2</span><span class="v">DHARWAD</span></div></div></td></tr><tr><td class="lable"><div>*Distance and Journey hours are approximate.</div></td></tr><tr><td class="lable"><div>**Concessions and Levies are applicable as per rules.</div></td></tr><tr><td></td></tr><tr><td></td></tr></table></td></tr></table></td><td></td></tr><tr><td></td><td><div><table><tr><td></td><td></td><td><div><a class="dboxheader"></a></div></td><td></td><td></td></tr></table></div></td><td></td></tr></table></td></tr></table></td></tr></tbody></table></body></html>'
        
        explanation_6 = "Missing and malformed fields are corrected using detail table values; duration is inferred from time difference."
        
        json_output_6 = {
            "explanation": explanation_6,
            "operator": "AIRAVAT",
            "bus_type": "AC Sleeper 2X1",
            "trip_code": "2345AIRBANGD01A",
            "route_code": "S78A",
            "departure_time": "23:45",
            "arrival_time": "05:15",
            "duration": "5.50",
            "price_in_rs": 750,
            "seats_available": 0,
            "via_route": ["TUMKUR"],
            "total_kms": "420.00",
            "child_fare": "0"
        }

        examples = [
            {"main": main_html_0, "detail": detail_html_0, "json": json_output_0},
            {"main": main_html_1, "detail": detail_html_1, "json": json_output_1},
            # {"main": main_html_2, "detail": detail_html_2, "json": json_output_2},
            # {"main": main_html_3, "detail": detail_html_3, "json": json_output_3},
            # {"main": main_html_4, "detail": detail_html_4, "json": json_output_4},
            # {"main": main_html_5, "detail": detail_html_5, "json": json_output_5},
            {"main": main_html_6, "detail": detail_html_6, "json": json_output_6},
        ]

        return [
            {
                "main_html": item["main"],
                "detail_html": item["detail"],
                "json_output": item["json"],
            }
            for item in examples
        ]