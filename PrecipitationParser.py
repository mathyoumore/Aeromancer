import re
import json

class PrecipitationParser:

    sentence_tokenizer = r'(\s*[^.!?]*[.!?]{1,3})'
    event_tokenizer = (
        r"(?P<precipitation>sleet|snow|ice)(fall)?\saccumulations?\s?((of( around)?|between|around|from)\s)?"
        r"(?P<accumulation_range_qualifier>(up to|a coating)\s)?(\sup)?\s?"
        r"(?P<accumulation_range_lower>(an?)|\d{1,3}|one|two|three|four|five|six|seven|eight|nine)?\s?"
        r"(?P<accumulation_denom_1>half|halves|tenths?|quarters?|hundredths?)?\s?"
        r"(?P<accumulation_denom_1_share>to|of|or|and|an?)?\s?"
        r"(?P<accumulation_range_upper>(an?)|\d{1,3}|one|two|three|four|five|six|seven|eight|nine)?\s"
        r"(?P<accumulation_denom_2>half|halves|tenths?|quarters?|hundredths?)?\s?(to|of|or|and)?\s?"
        r"(?P<accumulation_unit>inch|inches|an inch|a foot)?"
        r"(?P<or_two> or two)?"
        r"(?P<or_less> or less)?"
    )

    def __init__ (self):
        self.dump()

    def load_description(self, description):
        assert self.initialized == False, "I already have data! Please dump before loading new data."
        self.initialized = True
        self.description = description
        self.tokenized_precipitation, self.tokenized_sentences = self.tokenize()

    def dump(self):
        self.initialized = False
        self.tokenized_precipitation, self.tokenized_sentences, self.description = None, None, None
        self.event_data = {}

    def tokenize(self):
        matches = []
        simple_desc = re.sub("\n", ' ', str(self.description))
        sentences = re.split(self.sentence_tokenizer, simple_desc)
        for s in sentences:
            for m in re.finditer(self.event_tokenizer, s, flags = re.IGNORECASE):
                matches.append(m)
        return matches, sentences

    def str_to_num(self, raw_, none_result = 0):
        if raw_ is None:
            return none_result
        raw = raw_.strip()
        if len(raw) == 0:
            return none_result
        result = 0
        if re.search('\d',raw) is not None:
            result = float(raw)
        if raw[-1] == 's':
                raw = raw[:-1] # de-pluralizer
        match raw:
            case 'a':
                result = 1.0
            case 'an':
                result = 1.0
            case 'one':
                result = 1.0
            case 'two':
                result = 2.0
            case 'three':
                result = 3.0
            case 'four':
                result = 4.0
            case 'five':
                result = 5.0
            case 'six':
                result = 6.0
            case 'seven':
                result = 7.0
            case 'eight':
                result = 8.0
            case 'nine':
                result = 9.0
            case 'half':
                result = 0.5
            case 'quarter':
                result = 0.25
            case 'fifth':
                result = 0.2
            case 'tenth':
                result = 0.1
            case 'hundredth':
                result = 0.01
        return result

    def process(self):
        assert self.initialized == True, "You have to load_description first!"
        event_data = {}
        for precip in self.tokenized_precipitation:
            precip_dict = precip.groupdict()
            p_type = precip_dict['precipitation'].lower()
            p_min = self.str_to_num(precip_dict['accumulation_range_lower'])
            p_max = self.str_to_num(precip_dict['accumulation_range_upper'])
            p_min_mod = self.str_to_num(precip_dict['accumulation_denom_1'], 1)
            p_max_mod = self.str_to_num(precip_dict['accumulation_denom_2'], 1)

            event_data = {p_type + '_min': round(p_min * p_min_mod,2)} | event_data
            event_data = {p_type + '_max': round(p_max * p_max_mod,2)} | event_data

            # Scenario like "Up to a tenth of an inch"
            if precip_dict['accumulation_denom_1'] is not None:
                if (~precip_dict['accumulation_denom_1'].isnumeric() and
                precip_dict['accumulation_denom_1_share'] is not None):
                    p_max = 0

            # Scenario like "an inch of two"
            if precip_dict['or_two'] is not None:
                event_data[p_type + '_min'] = 1.0
                event_data[p_type + '_max'] = 2.0

            # Scenario like "an inch of less" or "up to 2 inches"
            if (precip_dict['or_less'] is not None or
            (precip_dict['accumulation_range_qualifier'] is not None and p_max == 0)):
                event_data[p_type + '_max'] = event_data[p_type + '_min']
                event_data[p_type + '_min'] = 0.0

        self.event_data = event_data
        return event_data
