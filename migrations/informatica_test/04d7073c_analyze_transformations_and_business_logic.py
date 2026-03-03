import xml.etree.ElementTree as ET
import json
import re
from pathlib import Path
from typing import Dict, List, Any, Tuple, Set
from dataclasses import dataclass, asdict, field
from collections import defaultdict, Counter
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ExpressionTransformation:
    """Represents an Expression transformation with its formulas"""
    name: str
    description: str
    ports: List[Dict[str, Any]]
    expressions: List[Dict[str, str]]
    pyspark_equivalent: str = ""
    complexity_score: int = 0
    categories: List[str] = field(default_factory=list)


@dataclass
class AggregatorTransformation:
    """Represents an Aggregator transformation with grouping logic"""
    name: str
    description: str
    group_by_ports: List[str]
    aggregate_expressions: List[Dict[str, str]]
    sorted_input: bool
    pyspark_equivalent: str = ""
    aggregate_functions: List[str] = field(default_factory=list)


@dataclass
class JoinerTransformation:
    """Represents a Joiner transformation with join conditions"""
    name: str
    description: str
    master_source: str
    detail_source: str
    join_type: str
    join_condition: str
    sorted: bool
    cache_size: str
    pyspark_equivalent: str = ""


@dataclass
class FilterTransformation:
    """Represents a Filter transformation with conditions"""
    name: str
    description: str
    filter_condition: str
    ports: List[Dict[str, Any]]
    pyspark_equivalent: str = ""
    condition_complexity: int = 0


@dataclass
class LookupTransformation:
    """Represents a Lookup transformation with caching strategy"""
    name: str
    description: str
    lookup_table: str
    lookup_condition: str
    return_ports: List[str]
    cache_type: str
    cache_size: str
    lookup_sql: str
    pyspark_equivalent: str = ""
    cache_strategy: str = ""


@dataclass
class RouterTransformation:
    """Represents a Router transformation with routing groups"""
    name: str
    description: str
    routing_groups: List[Dict[str, str]]
    default_group: bool
    pyspark_equivalent: str = ""


@dataclass
class SorterTransformation:
    """Represents a Sorter transformation"""
    name: str
    description: str
    sort_keys: List[Dict[str, str]]
    distinct: bool
    case_sensitive: bool
    pyspark_equivalent: str = ""


@dataclass
class UnionTransformation:
    """Represents a Union transformation"""
    name: str
    description: str
    input_groups: List[str]
    ports: List[Dict[str, Any]]
    pyspark_equivalent: str = ""


@dataclass
class CustomTransformation:
    """Represents a Custom transformation with code"""
    name: str
    description: str
    transformation_type: str
    custom_code: str
    language: str
    complexity_assessment: str = ""
    pyspark_equivalent: str = ""


class InformaticaExpressionParser:
    """Parser for Informatica expression syntax"""
    
    FUNCTION_MAPPING = {
        'TO_DATE': 'F.to_date',
        'TO_CHAR': 'F.date_format',
        'TO_NUMBER': 'F.col().cast("double")',
        'SUBSTR': 'F.substring',
        'INSTR': 'F.instr',
        'LENGTH': 'F.length',
        'LTRIM': 'F.ltrim',
        'RTRIM': 'F.rtrim',
        'TRIM': 'F.trim',
        'UPPER': 'F.upper',
        'LOWER': 'F.lower',
        'CONCAT': 'F.concat',
        'NVL': 'F.coalesce',
        'IIF': 'F.when().otherwise()',
        'DECODE': 'F.when().when().otherwise()',
        'ADD_TO_DATE': 'F.date_add / F.date_sub',
        'SYSDATE': 'F.current_date()',
        'SYSTIMESTAMP': 'F.current_timestamp()',
        'ROUND': 'F.round',
        'TRUNC': 'F.trunc',
        'ABS': 'F.abs',
        'CEIL': 'F.ceil',
        'FLOOR': 'F.floor',
        'MOD': 'F.col() % value',
        'POWER': 'F.pow',
        'SQRT': 'F.sqrt',
        'SUM': 'F.sum',
        'AVG': 'F.avg',
        'COUNT': 'F.count',
        'MAX': 'F.max',
        'MIN': 'F.min',
        'FIRST': 'F.first',
        'LAST': 'F.last',
        'ISNULL': 'F.col().isNull()',
        'IS_DATE': 'F.col().cast("date").isNotNull()',
        'IS_NUMBER': 'F.col().cast("double").isNotNull()',
        'REG_EXTRACT': 'F.regexp_extract',
        'REG_REPLACE': 'F.regexp_replace',
        'REG_MATCH': 'F.rlike'
    }
    
    @staticmethod
    def categorize_expression(expression: str) -> List[str]:
        """Categorize expression by type"""
        categories = []
        expr_upper = expression.upper()
        
        if any(func in expr_upper for func in ['TO_DATE', 'TO_CHAR', 'ADD_TO_DATE', 'SYSDATE']):
            categories.append('DATE_OPERATIONS')
        if any(func in expr_upper for func in ['SUBSTR', 'INSTR', 'LENGTH', 'TRIM', 'CONCAT']):
            categories.append('STRING_OPERATIONS')
        if any(func in expr_upper for func in ['TO_NUMBER', 'ROUND', 'TRUNC', 'ABS', 'MOD']):
            categories.append('NUMERIC_OPERATIONS')
        if any(func in expr_upper for func in ['IIF', 'DECODE']):
            categories.append('CONDITIONAL_LOGIC')
        if any(func in expr_upper for func in ['NVL', 'ISNULL']):
            categories.append('NULL_HANDLING')
        if any(func in expr_upper for func in ['REG_EXTRACT', 'REG_REPLACE', 'REG_MATCH']):
            categories.append('REGEX_OPERATIONS')
        if any(func in expr_upper for func in ['SUM', 'AVG', 'COUNT', 'MAX', 'MIN']):
            categories.append('AGGREGATION')
        
        return categories if categories else ['GENERAL']
    
    @staticmethod
    def calculate_complexity(expression: str) -> int:
        """Calculate complexity score for expression"""
        score = 0
        expr_upper = expression.upper()
        
        # Nested function calls
        score += expr_upper.count('(') * 2
        
        # Conditional logic
        score += expr_upper.count('IIF') * 5
        score += expr_upper.count('DECODE') * 7
        
        # String operations
        score += len(re.findall(r'SUBSTR|INSTR|CONCAT', expr_upper)) * 2
        
        # Date operations
        score += len(re.findall(r'TO_DATE|ADD_TO_DATE', expr_upper)) * 3
        
        # Regex operations
        score += len(re.findall(r'REG_\w+', expr_upper)) * 4
        
        # Aggregations
        score += len(re.findall(r'SUM|AVG|COUNT|MAX|MIN', expr_upper)) * 3
        
        return score
    
    @staticmethod
    def convert_to_pyspark(expression: str, port_name: str) -> str:
        """Convert Informatica expression to PySpark equivalent"""
        pyspark_expr = expression
        
        # Handle IIF conversion
        iif_pattern = r'IIF\s*\((.*?),(.*?),(.*?)\)'
        pyspark_expr = re.sub(
            iif_pattern,
            r'F.when(\1, \2).otherwise(\3)',
            pyspark_expr,
            flags=re.IGNORECASE
        )
        
        # Handle NVL conversion
        nvl_pattern = r'NVL\s*\((.*?),(.*?)\)'
        pyspark_expr = re.sub(
            nvl_pattern,
            r'F.coalesce(\1, \2)',
            pyspark_expr,
            flags=re.IGNORECASE
        )
        
        # Handle DECODE conversion
        decode_pattern = r'DECODE\s*\((.*?)\)'
        if re.search(decode_pattern, pyspark_expr, re.IGNORECASE):
            pyspark_expr = "# Complex DECODE - requires manual conversion to nested F.when().when().otherwise()"
        
        # Handle date functions
        pyspark_expr = re.sub(
            r'TO_DATE\s*\((.*?),\s*[\'"]?(.*?)[\'"]?\)',
            r'F.to_date(\1, "\2")',
            pyspark_expr,
            flags=re.IGNORECASE
        )
        
        # Handle string functions
        pyspark_expr = re.sub(r'SUBSTR\s*\(', r'F.substring(', pyspark_expr, flags=re.IGNORECASE)
        pyspark_expr = re.sub(r'LENGTH\s*\(', r'F.length(', pyspark_expr, flags=re.IGNORECASE)
        pyspark_expr = re.sub(r'UPPER\s*\(', r'F.upper(', pyspark_expr, flags=re.IGNORECASE)
        pyspark_expr = re.sub(r'LOWER\s*\(', r'F.lower(', pyspark_expr, flags=re.IGNORECASE)
        pyspark_expr = re.sub(r'TRIM\s*\(', r'F.trim(', pyspark_expr, flags=re.IGNORECASE)
        
        # Wrap in withColumn statement
        return f'df.withColumn("{port_name}", {pyspark_expr})'


class InformaticaTransformationAnalyzer:
    """Analyzes Informatica transformations and generates PySpark equivalents"""
    
    def __init__(self, xml_directory: str, output_directory: str):
        self.xml_directory = Path(xml_directory)
        self.output_directory = Path(output_directory)
        self.output_directory.mkdir(parents=True, exist_ok=True)
        
        self.expression_parser = InformaticaExpressionParser()
        
        self.transformations = {
            'expression': [],
            'aggregator': [],
            'joiner': [],
            'filter': [],
            'lookup': [],
            'router': [],
            'sorter': [],
            'union': [],
            'custom': []
        }
        
        self.transformation_counts = Counter()
        self.transformation_patterns = defaultdict(list)
        self.business_logic_catalog = []
    
    def analyze_all_mappings(self):
        """Analyze all mapping XML files in directory"""
        logger.info(f"Starting analysis of mappings in {self.xml_directory}")
        
        xml_files = list(self.xml_directory.glob("*.xml"))
        logger.info(f"Found {len(xml_files)} XML files to process")
        
        for xml_file in xml_files:
            try:
                logger.info(f"Processing {xml_file.name}")
                self._parse_mapping_xml(xml_file)
            except Exception as e:
                logger.error(f"Error processing {xml_file.name}: {str(e)}")
        
        self._generate_reports()
        self._identify_patterns()
        self._export_pyspark_templates()
    
    def _parse_mapping_xml(self, xml_file: Path):
        """Parse individual mapping XML file"""
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
            # Parse different transformation types
            for transform in root.findall('.//TRANSFORMATION'):
                transform_type = transform.get('TYPE', '').lower()
                transform_name = transform.get('NAME', '')
                
                self.transformation_counts[transform_type] += 1
                
                if transform_type == 'expression':
                    self._parse_expression_transformation(transform, xml_file.name)
                elif transform_type == 'aggregator':
                    self._parse_aggregator_transformation(transform, xml_file.name)
                elif transform_type == 'joiner':
                    self._parse_joiner_transformation(transform, xml_file.name)
                elif transform_type == 'filter':
                    self._parse_filter_transformation(transform, xml_file.name)
                elif transform_type == 'lookup procedure':
                    self._parse_lookup_transformation(transform, xml_file.name)
                elif transform_type == 'router':
                    self._parse_router_transformation(transform, xml_file.name)
                elif transform_type == 'sorter':
                    self._parse_sorter_transformation(transform, xml_file.name)
                elif transform_type == 'union':
                    self._parse_union_transformation(transform, xml_file.name)
                elif transform_type in ['custom', 'java', 'external procedure']:
                    self._parse_custom_transformation(transform, xml_file.name)
        
        except ET.ParseError as e:
            logger.error(f"XML parsing error in {xml_file.name}: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error parsing {xml_file.name}: {str(e)}")
    
    def _parse_expression_transformation(self, transform: ET.Element, source_file: str):
        """Parse Expression transformation"""
        name = transform.get('NAME', '')
        description = transform.get('DESCRIPTION', '')
        
        ports = []
        expressions = []
        
        for port in transform.findall('.//TRANSFORMFIELD'):
            port_info = {
                'name': port.get('NAME', ''),
                'datatype': port.get('DATATYPE', ''),
                'precision': port.get('PRECISION', ''),
                'scale': port.get('SCALE', ''),
                'port_type': port.get('PORTTYPE', ''),
                'expression': port.get('EXPRESSION', '')
            }
            ports.append(port_info)
            
            if port_info['expression']:
                expr_info = {
                    'port_name': port_info['name'],
                    'expression': port_info['expression'],
                    'datatype': port_info['datatype']
                }
                expressions.append(expr_info)
        
        # Generate PySpark equivalent
        pyspark_code = self._generate_expression_pyspark(expressions)
        
        # Calculate complexity and categorize
        total_complexity = sum(
            self.expression_parser.calculate_complexity(expr['expression'])
            for expr in expressions
        )
        
        categories = set()
        for expr in expressions:
            categories.update(self.expression_parser.categorize_expression(expr['expression']))
        
        expr_transform = ExpressionTransformation(
            name=name,
            description=description,
            ports=ports,
            expressions=expressions,
            pyspark_equivalent=pyspark_code,
            complexity_score=total_complexity,
            categories=list(categories)
        )
        
        self.transformations['expression'].append(expr_transform)
        
        # Document complex business logic
        if total_complexity > 20:
            self.business_logic_catalog.append({
                'type': 'expression',
                'name': name,
                'source_file': source_file,
                'complexity': total_complexity,
                'expressions': expressions,
                'categories': list(categories)
            })
    
    def _parse_aggregator_transformation(self, transform: ET.Element, source_file: str):
        """Parse Aggregator transformation"""
        name = transform.get('NAME', '')
        description = transform.get('DESCRIPTION', '')
        sorted_input = transform.get('SORTEDINPUT', 'NO') == 'YES'
        
        group_by_ports = []
        aggregate_expressions = []
        aggregate_functions = []
        
        for port in transform.findall('.//TRANSFORMFIELD'):
            port_name = port.get('NAME', '')
            port_type = port.get('PORTTYPE', '')
            expression = port.get('EXPRESSION', '')
            group_by = port.get('GROUPBY', 'NO') == 'YES'
            
            if group_by:
                group_by_ports.append(port_name)
            
            if expression and port_type == 'OUTPUT':
                agg_info = {
                    'port_name': port_name,
                    'expression': expression,
                    'datatype': port.get('DATATYPE', '')
                }
                aggregate_expressions.append(agg_info)
                
                # Extract aggregate function type
                expr_upper = expression.upper()
                for func in ['SUM', 'AVG', 'COUNT', 'MAX', 'MIN', 'FIRST', 'LAST', 'STDDEV', 'VARIANCE']:
                    if func in expr_upper:
                        aggregate_functions.append(func)
        
        # Generate PySpark equivalent
        pyspark_code = self._generate_aggregator_pyspark(
            group_by_ports, aggregate_expressions, sorted_input
        )
        
        agg_transform = AggregatorTransformation(
            name=name,
            description=description,
            group_by_ports=group_by_ports,
            aggregate_expressions=aggregate_expressions,
            sorted_input=sorted_input,
            pyspark_equivalent=pyspark_code,
            aggregate_functions=list(set(aggregate_functions))
        )
        
        self.transformations['aggregator'].append(agg_transform)
        
        # Document complex aggregations
        if len(aggregate_expressions) > 5 or len(group_by_ports) > 5:
            self.business_logic_catalog.append({
                'type': 'aggregator',
                'name': name,
                'source_file': source_file,
                'group_by_count': len(group_by_ports),
                'aggregate_count': len(aggregate_expressions),
                'functions': list(set(aggregate_functions))
            })
    
    def _parse_joiner_transformation(self, transform: ET.Element, source_file: str):
        """Parse Joiner transformation"""
        name = transform.get('NAME', '')
        description = transform.get('DESCRIPTION', '')
        join_type = transform.get('JOINTYPE', 'NORMAL')
        sorted_input = transform.get('SORTEDINPUT', 'NO') == 'YES'
        cache_size = transform.get('CACHESIZE', 'AUTO')
        
        master_source = ''
        detail_source = ''
        join_condition = transform.get('JOINCONDITION', '')
        
        # Parse source information from connectors
        for connector in transform.findall('.//CONNECTOR'):
            from_instance = connector.get('FROMINSTANCE', '')
            to_instance = connector.get('TOINSTANCE', '')
            
            if 'MASTER' in from_instance.upper():
                master_source = from_instance
            elif 'DETAIL' in from_instance.upper():
                detail_source = from_instance
        
        # Map join type to PySpark
        join_type_map = {
            'NORMAL': 'inner',
            'MASTER OUTER': 'left',
            'DETAIL OUTER': 'right',
            'FULL OUTER': 'outer'
        }
        pyspark_join_type = join_type_map.get(join_type.upper(), 'inner')
        
        # Generate PySpark equivalent
        pyspark_code = self._generate_joiner_pyspark(
            master_source, detail_source, join_condition, pyspark_join_type, sorted_input
        )
        
        joiner_transform = JoinerTransformation(
            name=name,
            description=description,
            master_source=master_source,
            detail_source=detail_source,
            join_type=join_type,
            join_condition=join_condition,
            sorted=sorted_input,
            cache_size=cache_size,
            pyspark_equivalent=pyspark_code
        )
        
        self.transformations['joiner'].append(joiner_transform)
        
        # Document complex joins
        if 'AND' in join_condition.upper() or 'OR' in join_condition.upper():
            self.business_logic_catalog.append({
                'type': 'joiner',
                'name': name,
                'source_file': source_file,
                'join_type': join_type,
                'condition': join_condition,
                'complexity': 'COMPLEX_CONDITION'
            })
    
    def _parse_filter_transformation(self, transform: ET.Element, source_file: str):
        """Parse Filter transformation"""
        name = transform.get('NAME', '')
        description = transform.get('DESCRIPTION', '')
        filter_condition = transform.get('FILTERCONDITION', '')
        
        ports = []
        for port in transform.findall('.//TRANSFORMFIELD'):
            port_info = {
                'name': port.get('NAME', ''),
                'datatype': port.get('DATATYPE', ''),
                'port_type': port.get('PORTTYPE', '')
            }
            ports.append(port_info)
        
        # Calculate condition complexity
        condition_complexity = self.expression_parser.calculate_complexity(filter_condition)
        
        # Generate PySpark equivalent
        pyspark_code = self._generate_filter_pyspark(filter_condition)
        
        filter_transform = FilterTransformation(
            name=name,
            description=description,
            filter_condition=filter_condition,
            ports=ports,
            pyspark_equivalent=pyspark_code,
            condition_complexity=condition_complexity
        )
        
        self.transformations['filter'].append(filter_transform)
        
        # Document complex filters
        if condition_complexity > 15:
            self.business_logic_catalog.append({
                'type': 'filter',
                'name': name,
                'source_file': source_file,
                'condition': filter_condition,
                'complexity': condition_complexity
            })
    
    def _parse_lookup_transformation(self, transform: ET.Element, source_file: str):
        """Parse Lookup transformation"""
        name = transform.get('NAME', '')
        description = transform.get('DESCRIPTION', '')
        lookup_table = transform.get('LOOKUPTABLE', '')
        cache_type = transform.get('CACHETYPE', 'AUTO')
        cache_size = transform.get('CACHESIZE', 'AUTO')
        
        lookup_condition = ''
        return_ports = []
        lookup_sql = transform.get('LOOKUPOVERRIDE', '')
        
        for port in transform.findall('.//TRANSFORMFIELD'):
            port_type = port.get('PORTTYPE', '')
            if port_type == 'LOOKUP':
                lookup_condition = port.get('EXPRESSION', '')
            elif port_type == 'RETURN':
                return_ports.append(port.get('NAME', ''))
        
        # Determine caching strategy
        cache_strategy = self._determine_cache_strategy(cache_type, cache_size)
        
        # Generate PySpark equivalent
        pyspark_code = self._generate_lookup_pyspark(
            lookup_table, lookup_condition, return_ports, cache_strategy, lookup_sql
        )
        
        lookup_transform = LookupTransformation(
            name=name,
            description=description,
            lookup_table=lookup_table,
            lookup_condition=lookup_condition,
            return_ports=return_ports,
            cache_type=cache_type,
            cache_size=cache_size,
            lookup_sql=lookup_sql,
            pyspark_equivalent=pyspark_code,
            cache_strategy=cache_strategy
        )
        
        self.transformations['lookup'].append(lookup_transform)
        
        # Document all lookups
        self.business_logic_catalog.append({
            'type': 'lookup',
            'name': name,
            'source_file': source_file,
            'table': lookup_table,
            'condition': lookup_condition,
            'cache_strategy': cache_strategy,
            'has_override': bool(lookup_sql)
        })
    
    def _parse_router_transformation(self, transform: ET.Element, source_file: str):
        """Parse Router transformation"""
        name = transform.get('NAME', '')
        description = transform.get('DESCRIPTION', '')
        
        routing_groups = []
        default_group = False
        
        for group in transform.findall('.//ROUTERGROUP'):
            group_name = group.get('GROUPNAME', '')
            group_condition = group.get('CONDITION', '')
            group_order = group.get('GROUPORDER', '0')
            
            if group_name.upper() == 'DEFAULT':
                default_group = True
            
            routing_groups.append({
                'name': group_name,
                'condition': group_condition,
                'order': group_order
            })
        
        # Generate PySpark equivalent
        pyspark_code = self._generate_router_pyspark(routing_groups, default_group)
        
        router_transform = RouterTransformation(
            name=name,
            description=description,
            routing_groups=routing_groups,
            default_group=default_group,
            pyspark_equivalent=pyspark_code
        )
        
        self.transformations['router'].append(router_transform)
        
        # Document complex routers
        if len(routing_groups) > 3:
            self.business_logic_catalog.append({
                'type': 'router',
                'name': name,
                'source_file': source_file,
                'group_count': len(routing_groups),
                'has_default': default_group,
                'groups': routing_groups
            })
    
    def _parse_sorter_transformation(self, transform: ET.Element, source_file: str):
        """Parse Sorter transformation"""
        name = transform.get('NAME', '')
        description = transform.get('DESCRIPTION', '')
        distinct = transform.get('DISTINCT', 'NO') == 'YES'
        case_sensitive = transform.get('CASESENSITIVE', 'YES') == 'YES'
        
        sort_keys = []
        for port in transform.findall('.//TRANSFORMFIELD'):
            sort_key = port.get('SORTKEY', '')
            if sort_key:
                sort_keys.append({
                    'name': port.get('NAME', ''),
                    'direction': port.get('SORTDIRECTION', 'ASCENDING'),
                    'order': sort_key
                })
        
        # Generate PySpark equivalent
        pyspark_code = self._generate_sorter_pyspark(sort_keys, distinct)
        
        sorter_transform = SorterTransformation(
            name=name,
            description=description,
            sort_keys=sort_keys,
            distinct=distinct,
            case_sensitive=case_sensitive,
            pyspark_equivalent=pyspark_code
        )
        
        self.transformations['sorter'].append(sorter_transform)
    
    def _parse_union_transformation(self, transform: ET.Element, source_file: str):
        """Parse Union transformation"""
        name = transform.get('NAME', '')
        description = transform.get('DESCRIPTION', '')
        
        input_groups = []
        ports = []
        
        for group in transform.findall('.//INPUTGROUP'):
            input_groups.append(group.get('NAME', ''))
        
        for port in transform.findall('.//TRANSFORMFIELD'):
            ports.append({
                'name': port.get('NAME', ''),
                'datatype': port.get('DATATYPE', ''),
                'port_type': port.get('PORTTYPE', '')
            })
        
        # Generate PySpark equivalent
        pyspark_code = self._generate_union_pyspark(input_groups, len(ports))
        
        union_transform = UnionTransformation(
            name=name,
            description=description,
            input_groups=input_groups,
            ports=ports,
            pyspark_equivalent=pyspark_code
        )
        
        self.transformations['union'].append(union_transform)
    
    def _parse_custom_transformation(self, transform: ET.Element, source_file: str):
        """Parse Custom transformation"""
        name = transform.get('NAME', '')
        description = transform.get('DESCRIPTION', '')
        transform_type = transform.get('TYPE', '')
        
        custom_code = ''
        language = 'UNKNOWN'
        
        # Extract custom code from different possible locations
        code_element = transform.find('.//CODE')
        if code_element is not None:
            custom_code = code_element.text or ''
            language = code_element.get('LANGUAGE', 'UNKNOWN')
        
        # Assess complexity
        complexity_assessment = self._assess_custom_complexity(custom_code, language)
        
        custom_transform = CustomTransformation(
            name=name,
            description=description,
            transformation_type=transform_type,
            custom_code=custom_code,
            language=language,
            complexity_assessment=complexity_assessment,
            pyspark_equivalent="# Requires manual conversion - review custom code"
        )
        
        self.transformations['custom'].append(custom_transform)
        
        # Always document custom transformations
        self.business_logic_catalog.append({
            'type': 'custom',
            'name': name,
            'source_file': source_file,
            'language': language,
            'complexity': complexity_assessment,
            'code_length': len(custom_code)
        })
    
    def _generate_expression_pyspark(self, expressions: List[Dict[str, str]]) -> str:
        """Generate PySpark code for Expression transformation"""
        if not expressions:
            return "# No expressions to convert"
        
        code_lines = [
            "# Expression Transformation - PySpark Equivalent",
            "from pyspark.sql import functions as F",
            "",
            "df = df"
        ]
        
        for expr in expressions:
            port_name = expr['port_name']
            expression = expr['expression']
            pyspark_expr = self.expression_parser.convert_to_pyspark(expression, port_name)
            code_lines.append(f"  # {port_name}: {expression}")
            code_lines.append(f"  .{pyspark_expr.split('.', 1)[1]}")
        
        return '\n'.join(code_lines)
    
    def _generate_aggregator_pyspark(
        self, 
        group_by_ports: List[str], 
        aggregate_expressions: List[Dict[str, str]],
        sorted_input: bool
    ) -> str:
        """Generate PySpark code for Aggregator transformation"""
        code_lines = [
            "# Aggregator Transformation - PySpark Equivalent",
            "from pyspark.sql import functions as F",
            ""
        ]
        
        if group_