import xml.etree.ElementTree as ET
import os
import json
import re
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class TransformationMetadata:
    """Metadata for transformation analysis"""
    transformation_name: str
    transformation_type: str
    description: str
    business_logic: str
    complexity_score: int
    source_objects: List[str]
    target_objects: List[str]
    input_ports: List[Dict[str, str]]
    output_ports: List[Dict[str, str]]
    properties: Dict[str, Any]


@dataclass
class ExpressionTransformation:
    """Expression transformation details"""
    name: str
    expressions: List[Dict[str, str]]
    port_mappings: List[Dict[str, str]]
    formulas: List[str]
    complexity: str
    pyspark_equivalent: str


@dataclass
class AggregatorTransformation:
    """Aggregator transformation details"""
    name: str
    group_by_ports: List[str]
    aggregate_expressions: List[Dict[str, str]]
    sorted: bool
    aggregate_cache: str
    pyspark_equivalent: str


@dataclass
class JoinerTransformation:
    """Joiner transformation details"""
    name: str
    join_type: str
    join_condition: str
    master_source: str
    detail_source: str
    sorted_input: bool
    cache_directory: str
    pyspark_equivalent: str


@dataclass
class FilterTransformation:
    """Filter transformation details"""
    name: str
    filter_condition: str
    filter_type: str
    route_groups: List[Dict[str, str]]
    pyspark_equivalent: str


@dataclass
class LookupTransformation:
    """Lookup transformation details"""
    name: str
    lookup_source: str
    lookup_condition: str
    lookup_type: str
    cache_type: str
    cache_size: str
    return_ports: List[str]
    pyspark_equivalent: str


@dataclass
class CustomTransformation:
    """Custom transformation details"""
    name: str
    transformation_type: str
    custom_code: str
    language: str
    complexity_assessment: str
    pyspark_equivalent: str


class InformaticaTransformationAnalyzer:
    """Comprehensive analyzer for Informatica transformations"""
    
    def __init__(self, xml_directory: str, output_directory: str):
        self.xml_directory = xml_directory
        self.output_directory = output_directory
        self.transformation_catalog = defaultdict(list)
        self.transformation_counts = defaultdict(int)
        self.business_logic_examples = []
        self.expression_formulas = defaultdict(list)
        self.join_conditions = []
        self.lookup_strategies = []
        self.custom_code_inventory = []
        self.transformation_patterns = defaultdict(list)
        
        os.makedirs(output_directory, exist_ok=True)
    
    def parse_mapping_xml_files(self) -> Dict[str, Any]:
        """Parse all mapping XML files and extract transformations"""
        logger.info(f"Parsing XML files from {self.xml_directory}")
        
        all_mappings = []
        
        for root_dir, dirs, files in os.walk(self.xml_directory):
            for file in files:
                if file.endswith('.xml') or file.endswith('.XML'):
                    file_path = os.path.join(root_dir, file)
                    try:
                        mapping_data = self._parse_single_xml(file_path)
                        all_mappings.append(mapping_data)
                        logger.info(f"Parsed {file}: {len(mapping_data.get('transformations', []))} transformations")
                    except Exception as e:
                        logger.error(f"Error parsing {file_path}: {str(e)}")
        
        return {
            'total_mappings': len(all_mappings),
            'mappings': all_mappings,
            'transformation_counts': dict(self.transformation_counts)
        }
    
    def _parse_single_xml(self, file_path: str) -> Dict[str, Any]:
        """Parse a single mapping XML file"""
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        mapping_data = {
            'file_name': os.path.basename(file_path),
            'file_path': file_path,
            'mapping_name': root.get('NAME', 'Unknown'),
            'transformations': []
        }
        
        # Parse all transformation types
        for transformation in root.findall('.//TRANSFORMATION'):
            trans_type = transformation.get('TYPE', 'Unknown')
            trans_name = transformation.get('NAME', 'Unknown')
            
            self.transformation_counts[trans_type] += 1
            
            trans_data = self._extract_transformation_data(transformation)
            mapping_data['transformations'].append(trans_data)
            self.transformation_catalog[trans_type].append(trans_data)
        
        return mapping_data
    
    def _extract_transformation_data(self, transformation: ET.Element) -> TransformationMetadata:
        """Extract comprehensive transformation data"""
        trans_type = transformation.get('TYPE', 'Unknown')
        trans_name = transformation.get('NAME', 'Unknown')
        description = transformation.get('DESCRIPTION', '')
        
        input_ports = self._extract_ports(transformation, 'INPUT')
        output_ports = self._extract_ports(transformation, 'OUTPUT')
        properties = self._extract_properties(transformation)
        
        business_logic = self._extract_business_logic(transformation, trans_type)
        complexity_score = self._calculate_complexity(transformation, trans_type)
        
        return TransformationMetadata(
            transformation_name=trans_name,
            transformation_type=trans_type,
            description=description,
            business_logic=business_logic,
            complexity_score=complexity_score,
            source_objects=self._extract_sources(transformation),
            target_objects=self._extract_targets(transformation),
            input_ports=input_ports,
            output_ports=output_ports,
            properties=properties
        )
    
    def _extract_ports(self, transformation: ET.Element, port_type: str) -> List[Dict[str, str]]:
        """Extract transformation ports"""
        ports = []
        for port in transformation.findall(f'.//TRANSFORMFIELD[@PORTTYPE="{port_type}"]'):
            port_data = {
                'name': port.get('NAME', ''),
                'datatype': port.get('DATATYPE', ''),
                'precision': port.get('PRECISION', ''),
                'scale': port.get('SCALE', ''),
                'expression': port.get('EXPRESSION', ''),
                'nullable': port.get('NULLABLE', 'TRUE')
            }
            ports.append(port_data)
        return ports
    
    def _extract_properties(self, transformation: ET.Element) -> Dict[str, Any]:
        """Extract transformation properties"""
        properties = {}
        for prop in transformation.findall('.//TABLEATTRIBUTE'):
            name = prop.get('NAME', '')
            value = prop.get('VALUE', '')
            properties[name] = value
        return properties
    
    def _extract_business_logic(self, transformation: ET.Element, trans_type: str) -> str:
        """Extract and document business logic"""
        logic = []
        
        if trans_type == 'Expression':
            for field in transformation.findall('.//TRANSFORMFIELD'):
                expr = field.get('EXPRESSION', '')
                if expr:
                    logic.append(f"{field.get('NAME')}: {expr}")
        
        elif trans_type == 'Filter':
            for field in transformation.findall('.//TRANSFORMFIELD'):
                expr = field.get('EXPRESSION', '')
                if expr and 'FILTER' in field.get('NAME', '').upper():
                    logic.append(f"Filter Condition: {expr}")
        
        elif trans_type == 'Aggregator':
            group_by = []
            aggregates = []
            for field in transformation.findall('.//TRANSFORMFIELD'):
                if field.get('GROUPBY') == 'YES':
                    group_by.append(field.get('NAME'))
                expr = field.get('EXPRESSION', '')
                if expr and any(agg in expr.upper() for agg in ['SUM', 'AVG', 'COUNT', 'MAX', 'MIN']):
                    aggregates.append(f"{field.get('NAME')}: {expr}")
            
            if group_by:
                logic.append(f"Group By: {', '.join(group_by)}")
            logic.extend(aggregates)
        
        return '\n'.join(logic)
    
    def _calculate_complexity(self, transformation: ET.Element, trans_type: str) -> int:
        """Calculate transformation complexity score"""
        complexity = 0
        
        # Base complexity by type
        complexity_weights = {
            'Expression': 2,
            'Aggregator': 4,
            'Joiner': 5,
            'Lookup': 4,
            'Router': 3,
            'Filter': 2,
            'Sorter': 2,
            'Union': 1,
            'Custom': 10
        }
        
        complexity += complexity_weights.get(trans_type, 1)
        
        # Add complexity for number of expressions
        expressions = transformation.findall('.//TRANSFORMFIELD[@EXPRESSION]')
        complexity += len(expressions)
        
        # Add complexity for nested functions
        for field in expressions:
            expr = field.get('EXPRESSION', '')
            nested_funcs = len(re.findall(r'\w+\([^)]*\(', expr))
            complexity += nested_funcs * 2
        
        return complexity
    
    def _extract_sources(self, transformation: ET.Element) -> List[str]:
        """Extract source objects"""
        sources = []
        for src in transformation.findall('.//SOURCE'):
            sources.append(src.get('NAME', ''))
        return sources
    
    def _extract_targets(self, transformation: ET.Element) -> List[str]:
        """Extract target objects"""
        targets = []
        for tgt in transformation.findall('.//TARGET'):
            targets.append(tgt.get('NAME', ''))
        return targets
    
    def document_expression_transformations(self) -> List[ExpressionTransformation]:
        """Document all expression transformations and formulas"""
        logger.info("Documenting Expression transformations")
        
        expressions = []
        
        for trans_data in self.transformation_catalog.get('Expression', []):
            expr_list = []
            formula_list = []
            
            for port in trans_data.output_ports:
                if port.get('expression'):
                    expr_dict = {
                        'port_name': port['name'],
                        'expression': port['expression'],
                        'datatype': port['datatype']
                    }
                    expr_list.append(expr_dict)
                    formula_list.append(port['expression'])
                    
                    # Categorize formulas
                    self._categorize_formula(port['expression'], trans_data.transformation_name)
            
            pyspark_code = self._convert_expressions_to_pyspark(expr_list)
            
            complexity = 'Low'
            if trans_data.complexity_score > 10:
                complexity = 'High'
            elif trans_data.complexity_score > 5:
                complexity = 'Medium'
            
            expression_trans = ExpressionTransformation(
                name=trans_data.transformation_name,
                expressions=expr_list,
                port_mappings=[{'input': p['name'], 'output': p['name']} for p in trans_data.input_ports],
                formulas=formula_list,
                complexity=complexity,
                pyspark_equivalent=pyspark_code
            )
            
            expressions.append(expression_trans)
        
        logger.info(f"Documented {len(expressions)} Expression transformations")
        return expressions
    
    def _categorize_formula(self, formula: str, trans_name: str):
        """Categorize formulas by type"""
        formula_upper = formula.upper()
        
        categories = {
            'String': ['SUBSTR', 'INSTR', 'LTRIM', 'RTRIM', 'UPPER', 'LOWER', 'CONCAT'],
            'Date': ['TO_DATE', 'SYSDATE', 'ADD_TO_DATE', 'TRUNC'],
            'Numeric': ['ROUND', 'CEIL', 'FLOOR', 'ABS', 'POWER'],
            'Conditional': ['IIF', 'DECODE', 'CASE'],
            'Null Handling': ['ISNULL', 'NVL', 'COALESCE'],
            'Aggregate': ['SUM', 'AVG', 'COUNT', 'MAX', 'MIN']
        }
        
        for category, keywords in categories.items():
            if any(keyword in formula_upper for keyword in keywords):
                self.expression_formulas[category].append({
                    'transformation': trans_name,
                    'formula': formula
                })
    
    def _convert_expressions_to_pyspark(self, expressions: List[Dict[str, str]]) -> str:
        """Convert Informatica expressions to PySpark equivalent"""
        pyspark_code = "# PySpark Expression Transformation Equivalent\n"
        pyspark_code += "df = df.withColumns({\n"
        
        for expr in expressions:
            port_name = expr['port_name']
            expression = expr['expression']
            
            pyspark_expr = self._translate_informatica_to_pyspark(expression)
            pyspark_code += f"    '{port_name}': {pyspark_expr},\n"
        
        pyspark_code += "})"
        
        return pyspark_code
    
    def _translate_informatica_to_pyspark(self, informatica_expr: str) -> str:
        """Translate Informatica expression to PySpark"""
        # Common Informatica to PySpark function mappings
        translations = {
            r'SYSDATE': 'F.current_timestamp()',
            r'TO_DATE\((.*?),\s*["\']([^"\']+)["\']\)': r'F.to_date(\1, "\2")',
            r'TO_CHAR\((.*?),\s*["\']([^"\']+)["\']\)': r'F.date_format(\1, "\2")',
            r'SUBSTR\((.*?),\s*(\d+),\s*(\d+)\)': r'F.substring(\1, \2, \3)',
            r'INSTR\((.*?),\s*(.*?)\)': r'F.locate(\2, \1)',
            r'LTRIM\((.*?)\)': r'F.ltrim(\1)',
            r'RTRIM\((.*?)\)': r'F.rtrim(\1)',
            r'UPPER\((.*?)\)': r'F.upper(\1)',
            r'LOWER\((.*?)\)': r'F.lower(\1)',
            r'LENGTH\((.*?)\)': r'F.length(\1)',
            r'ROUND\((.*?),\s*(\d+)\)': r'F.round(\1, \2)',
            r'TRUNC\((.*?)\)': r'F.trunc(\1)',
            r'NVL\((.*?),\s*(.*?)\)': r'F.coalesce(\1, \2)',
            r'DECODE\((.*?)\)': r'# DECODE requires manual conversion to CASE WHEN',
            r'IIF\((.*?),\s*(.*?),\s*(.*?)\)': r'F.when(\1, \2).otherwise(\3)',
            r'ISNULL\((.*?)\)': r'F.isnull(\1)',
            r'CONCAT\((.*?)\)': r'F.concat(\1)',
        }
        
        pyspark_expr = informatica_expr
        
        for pattern, replacement in translations.items():
            pyspark_expr = re.sub(pattern, replacement, pyspark_expr, flags=re.IGNORECASE)
        
        # Replace column references
        pyspark_expr = re.sub(r'\b([A-Z_][A-Z0-9_]*)\b', r'F.col("\1")', pyspark_expr)
        
        return pyspark_expr
    
    def analyze_aggregator_transformations(self) -> List[AggregatorTransformation]:
        """Analyze all aggregator transformations"""
        logger.info("Analyzing Aggregator transformations")
        
        aggregators = []
        
        for trans_data in self.transformation_catalog.get('Aggregator', []):
            group_by_ports = []
            agg_expressions = []
            
            for port in trans_data.output_ports:
                if port.get('expression'):
                    expr = port['expression']
                    if any(agg in expr.upper() for agg in ['SUM', 'AVG', 'COUNT', 'MAX', 'MIN', 'FIRST', 'LAST']):
                        agg_expressions.append({
                            'port_name': port['name'],
                            'expression': expr,
                            'aggregate_type': self._identify_aggregate_type(expr)
                        })
                
                # Check for group by ports
                if trans_data.properties.get(f'GROUPBY_{port["name"]}') == 'YES':
                    group_by_ports.append(port['name'])
            
            pyspark_code = self._convert_aggregator_to_pyspark(group_by_ports, agg_expressions)
            
            aggregator = AggregatorTransformation(
                name=trans_data.transformation_name,
                group_by_ports=group_by_ports,
                aggregate_expressions=agg_expressions,
                sorted=trans_data.properties.get('Sorted Input', 'NO') == 'YES',
                aggregate_cache=trans_data.properties.get('Aggregate Cache Size', 'Auto'),
                pyspark_equivalent=pyspark_code
            )
            
            aggregators.append(aggregator)
        
        logger.info(f"Analyzed {len(aggregators)} Aggregator transformations")
        return aggregators
    
    def _identify_aggregate_type(self, expression: str) -> str:
        """Identify the type of aggregate function"""
        expr_upper = expression.upper()
        
        agg_types = {
            'SUM': 'sum',
            'AVG': 'avg',
            'COUNT': 'count',
            'MAX': 'max',
            'MIN': 'min',
            'FIRST': 'first',
            'LAST': 'last',
            'STDDEV': 'stddev',
            'VARIANCE': 'variance'
        }
        
        for keyword, agg_type in agg_types.items():
            if keyword in expr_upper:
                return agg_type
        
        return 'unknown'
    
    def _convert_aggregator_to_pyspark(self, group_by_ports: List[str], agg_expressions: List[Dict]) -> str:
        """Convert Aggregator transformation to PySpark"""
        pyspark_code = "# PySpark Aggregator Transformation Equivalent\n"
        
        if group_by_ports:
            pyspark_code += f"df_aggregated = df.groupBy({', '.join([f'F.col(\"{col}\")' for col in group_by_ports])})\\\n"
        else:
            pyspark_code += "df_aggregated = df.agg(\n"
        
        agg_funcs = []
        for agg_expr in agg_expressions:
            port_name = agg_expr['port_name']
            agg_type = agg_expr['aggregate_type']
            expression = agg_expr['expression']
            
            # Extract column name from expression
            col_match = re.search(r'\((.*?)\)', expression)
            if col_match:
                col_name = col_match.group(1).strip()
                agg_funcs.append(f"    F.{agg_type}(F.col('{col_name}')).alias('{port_name}')")
        
        if group_by_ports:
            pyspark_code += "    .agg(\n" + ',\n'.join(agg_funcs) + "\n    )"
        else:
            pyspark_code += ',\n'.join(agg_funcs) + "\n)"
        
        return pyspark_code
    
    def map_joiner_transformations(self) -> List[JoinerTransformation]:
        """Map all joiner transformations and join conditions"""
        logger.info("Mapping Joiner transformations")
        
        joiners = []
        
        for trans_data in self.transformation_catalog.get('Joiner', []):
            join_type = trans_data.properties.get('Join Type', 'Normal')
            join_condition = trans_data.properties.get('Join Condition', '')
            master_source = trans_data.properties.get('Master Source', '')
            detail_source = trans_data.properties.get('Detail Source', '')
            sorted_input = trans_data.properties.get('Sorted Input', 'NO') == 'YES'
            cache_dir = trans_data.properties.get('Cache Directory', '')
            
            # Parse join condition
            join_condition = self._parse_join_condition(trans_data)
            
            pyspark_code = self._convert_joiner_to_pyspark(
                join_type, join_condition, master_source, detail_source
            )
            
            joiner = JoinerTransformation(
                name=trans_data.transformation_name,
                join_type=join_type,
                join_condition=join_condition,
                master_source=master_source,
                detail_source=detail_source,
                sorted_input=sorted_input,
                cache_directory=cache_dir,
                pyspark_equivalent=pyspark_code
            )
            
            joiners.append(joiner)
            self.join_conditions.append({
                'transformation': trans_data.transformation_name,
                'condition': join_condition,
                'type': join_type
            })
        
        logger.info(f"Mapped {len(joiners)} Joiner transformations")
        return joiners
    
    def _parse_join_condition(self, trans_data: TransformationMetadata) -> str:
        """Parse join condition from transformation data"""
        conditions = []
        
        # Look for join condition in properties or expressions
        for port in trans_data.output_ports:
            expr = port.get('expression', '')
            if '=' in expr and any(src in expr for src in trans_data.source_objects):
                conditions.append(expr)
        
        if not conditions:
            # Try to derive from port names
            master_ports = [p for p in trans_data.input_ports if 'MASTER' in p.get('name', '').upper()]
            detail_ports = [p for p in trans_data.input_ports if 'DETAIL' in p.get('name', '').upper()]
            
            for m_port in master_ports:
                for d_port in detail_ports:
                    if m_port['name'].replace('MASTER_', '') == d_port['name'].replace('DETAIL_', ''):
                        conditions.append(f"{m_port['name']} = {d_port['name']}")
        
        return ' AND '.join(conditions) if conditions else 'Not specified'
    
    def _convert_joiner_to_pyspark(self, join_type: str, join_condition: str, 
                                   master_source: str, detail_source: str) -> str:
        """Convert Joiner transformation to PySpark"""
        # Map Informatica join types to PySpark
        join_type_mapping = {
            'Normal': 'inner',
            'Master Outer': 'left',
            'Detail Outer': 'right',
            'Full Outer': 'outer'
        }
        
        pyspark_join_type = join_type_mapping.get(join_type, 'inner')
        
        pyspark_code = f"# PySpark Joiner Transformation Equivalent\n"
        pyspark_code += f"# Join Type: {join_type} -> {pyspark_join_type}\n"
        pyspark_code += f"df_master = df_{master_source}  # Master/Left source\n"
        pyspark_code += f"df_detail = df_{detail_source}  # Detail/Right source\n\n"
        
        # Parse join condition
        join_exprs = []
        if '=' in join_condition:
            conditions = join_condition.split(' AND ')
            for cond in conditions:
                if '=' in cond:
                    left, right = cond.split('=')
                    left = left.strip()
                    right = right.strip()
                    join_exprs.append(f"df_master['{left}'] == df_detail['{right}']")
        
        if join_exprs:
            join_condition_str = ' & '.join(join_exprs)
            pyspark_code += f"df_joined = df_master.join(df_detail, {join_condition_str}, '{pyspark_join_type}')"
        else:
            pyspark_code += f"# Manual join condition definition required\n"
            pyspark_code += f"df_joined = df_master.join(df_detail, <join_condition>, '{pyspark_join_type}')"
        
        return pyspark_code
    
    def document_filter_transformations(self) -> List[FilterTransformation]:
        """Document all filter conditions and routing logic"""
        logger.info("Documenting Filter and Router transformations")
        
        filters = []
        
        # Process Filter transformations
        for trans_data in self.transformation_catalog.get('Filter', []):
            filter_condition = ''
            
            for port in trans_data.output_ports:
                if 'FILTER' in port['name'].upper() or port.get('expression'):
                    filter_condition = port.get('expression', '')
                    break
            
            pyspark_code = self._convert_filter_to_pyspark(filter_condition)
            
            filter_trans = FilterTransformation(
                name=trans_data.transformation_name,
                filter_condition=filter_condition,
                filter_type='Filter',
                route_groups=[],
                pyspark_equivalent=pyspark_code
            )
            
            filters.append(filter_trans)
        
        # Process Router transformations
        for trans_data in self.transformation_catalog.get('Router', []):
            route_groups = []
            
            for port in trans_data.output_ports:
                if port.get('expression'):
                    route_groups.append({
                        'group_name': port['name'],
                        'condition': port['expression']
                    })
            
            pyspark_code = self._convert_router_to_pyspark(route_groups)
            
            router_trans = FilterTransformation(
                name=trans_data.transformation_name,
                filter_condition='Multiple conditions',
                filter_type='Router',
                route_groups=route_groups,
                pyspark_equivalent=pyspark_code
            )
            
            filters.append(router_trans)
        
        logger.info(f"Documented {len(filters)} Filter/Router transformations")
        return filters
    
    def _convert_filter_to_pyspark(self, filter_condition: str) -> str:
        """Convert Filter transformation to PySpark"""
        pyspark_code = "# PySpark Filter Transformation Equivalent\n"
        
        if filter_condition:
            pyspark_expr = self._translate_informatica_to_pyspark(filter_condition)
            pyspark_code += f"df_filtered = df.filter({pyspark_expr})"
        else:
            pyspark_code += "# Filter condition not specified"
        
        return pyspark_code
    
    def _convert_router_to_pyspark(self, route_groups: List[Dict]) -> str:
        """Convert Router transformation to PySpark"""
        pyspark_code = "# PySpark Router Transformation Equivalent\n"
        pyspark_code += "# Create multiple output DataFrames based on conditions\n\n"
        
        for i, group in enumerate(route_groups):
            group_name = group['group_name']
            condition = group['condition']
            pyspark_expr = self._translate_informatica_to_pyspark(condition)
            
            pyspark_code += f"df_{group_name} = df.filter({pyspark_expr})\n"
        
        # Add default group
        pyspark_code += "\n# Default group (records not matching any condition)\n"
        all_conditions = ' | '.join([f"({self._translate_informatica_to_pyspark(g['condition'])})" 
                                     for g in route_groups])
        pyspark_code += f"df_default = df.filter(~({all_conditions}))"
        
        return pyspark_code
    
    def identify_lookup_transformations(self) -> List[LookupTransformation]:
        """Identify and document lookup transformations and caching strategies"""
        logger.info("Identifying Lookup transformations")
        
        lookups = []
        
        for trans_data in self.transformation_catalog.get('Lookup', []):
            lookup_source = trans_data.properties.get('Lookup Source', '')
            lookup_type = trans_data.properties.get('Lookup Type', 'Connected')
            cache_type = trans_data.properties.get('Cache Type', 'Static')
            cache_size = trans_data.properties.get('Lookup Cache Size', 'Auto')
            
            # Extract lookup condition
            lookup_condition = self._extract_lookup_condition(trans_data)
            
            # Extract return ports
            return_ports = [p['name'] for p in trans_data.output_ports 
                          if 'RETURN' in p.get('name', '').upper()]
            
            pyspark_code = self._convert_lookup_to_pyspark(
                lookup_source, lookup_condition, return_ports, cache_type
            )
            
            lookup = LookupTransformation(
                name=trans_data.transformation_name,
                lookup_source=lookup_source,
                lookup_condition=lookup_condition,
                lookup_type=lookup_type,
                cache_type=cache_type,
                cache_size=cache_size,
                return_ports=return_ports,
                pyspark_equivalent=pyspark_code
            )
            
            lookups.append(lookup)
            self.lookup_strategies.append({
                'transformation': trans_data.transformation_name,
                'cache_type': cache_type,
                'cache_size': cache_size,
                'strategy': self._recommend_pyspark_lookup_strategy(cache_type, cache_size)
            })
        
        logger.info(f"Identified {len(lookups)} Lookup transformations")
        return lookups
    
    def _extract_lookup_condition(self, trans_data: TransformationMetadata) -> str:
        """Extract lookup condition from transformation"""
        conditions = []
        
        for port in trans_data.input_ports:
            if port.get('expression') and '=' in port['expression']:
                conditions.append(port['expression'])
        
        if not conditions:
            # Try to derive from port names
            for port in trans_data.input_ports:
                if 'KEY' in port['name'].upper():
                    conditions.append(