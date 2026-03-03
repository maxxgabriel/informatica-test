import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Dict, List, Tuple, Any, Optional
import json
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class InformaticaTransformationAnalyzer:
    """
    Comprehensive analyzer for Informatica transformations with PySpark code generation.
    Parses mapping XML files and extracts all transformation logic with PySpark equivalents.
    """
    
    def __init__(self, xml_path: str, output_dir: str = "./transformation_analysis"):
        """
        Initialize the transformation analyzer.
        
        Args:
            xml_path: Path to Informatica mapping XML file or directory
            output_dir: Directory for output analysis files
        """
        self.xml_path = Path(xml_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.transformation_catalog = defaultdict(list)
        self.expression_formulas = []
        self.aggregator_logic = []
        self.joiner_conditions = []
        self.filter_conditions = []
        self.lookup_transformations = []
        self.custom_transformations = []
        self.sorter_logic = []
        self.router_logic = []
        self.union_logic = []
        self.transformation_patterns = defaultdict(list)
        
        self.spark = SparkSession.builder \
            .appName("InformaticaTransformationAnalyzer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
    
    def parse_xml_files(self) -> None:
        """Parse all XML mapping files in the specified path."""
        xml_files = []
        
        if self.xml_path.is_file():
            xml_files = [self.xml_path]
        elif self.xml_path.is_dir():
            xml_files = list(self.xml_path.glob("**/*.xml"))
        
        logger.info(f"Found {len(xml_files)} XML files to process")
        
        for xml_file in xml_files:
            try:
                logger.info(f"Processing: {xml_file}")
                self._parse_mapping_xml(xml_file)
            except Exception as e:
                logger.error(f"Error processing {xml_file}: {str(e)}")
    
    def _parse_mapping_xml(self, xml_file: Path) -> None:
        """Parse individual mapping XML file and extract transformations."""
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        mapping_name = xml_file.stem
        
        # Extract all transformation types
        transformations = root.findall(".//TRANSFORMATION")
        
        for trans in transformations:
            trans_type = trans.get("TYPE", "UNKNOWN")
            trans_name = trans.get("NAME", "UNNAMED")
            
            self.transformation_catalog[trans_type].append({
                "mapping": mapping_name,
                "name": trans_name,
                "type": trans_type,
                "xml_file": str(xml_file)
            })
            
            # Route to specific parsers based on type
            if trans_type == "Expression":
                self._parse_expression_transformation(trans, mapping_name)
            elif trans_type == "Aggregator":
                self._parse_aggregator_transformation(trans, mapping_name)
            elif trans_type == "Joiner":
                self._parse_joiner_transformation(trans, mapping_name)
            elif trans_type == "Filter":
                self._parse_filter_transformation(trans, mapping_name)
            elif trans_type == "Lookup":
                self._parse_lookup_transformation(trans, mapping_name)
            elif trans_type == "Sorter":
                self._parse_sorter_transformation(trans, mapping_name)
            elif trans_type == "Router":
                self._parse_router_transformation(trans, mapping_name)
            elif trans_type == "Union":
                self._parse_union_transformation(trans, mapping_name)
            elif trans_type in ["Custom", "External Procedure"]:
                self._parse_custom_transformation(trans, mapping_name)
    
    def _parse_expression_transformation(self, trans_elem: ET.Element, mapping: str) -> None:
        """Extract and analyze Expression transformation logic."""
        trans_name = trans_elem.get("NAME")
        
        for port in trans_elem.findall(".//TRANSFORMFIELD"):
            port_name = port.get("NAME")
            port_type = port.get("PORTTYPE", "INPUT")
            datatype = port.get("DATATYPE")
            expression = port.get("EXPRESSION", "")
            
            if expression and port_type in ["OUTPUT", "VARIABLE"]:
                formula_info = {
                    "mapping": mapping,
                    "transformation": trans_name,
                    "port_name": port_name,
                    "port_type": port_type,
                    "datatype": datatype,
                    "expression": expression,
                    "complexity": self._assess_expression_complexity(expression),
                    "pyspark_equivalent": self._convert_expression_to_pyspark(expression, port_name)
                }
                
                self.expression_formulas.append(formula_info)
                
                # Identify patterns
                pattern_type = self._identify_expression_pattern(expression)
                self.transformation_patterns[pattern_type].append(formula_info)
    
    def _parse_aggregator_transformation(self, trans_elem: ET.Element, mapping: str) -> None:
        """Extract and analyze Aggregator transformation logic."""
        trans_name = trans_elem.get("NAME")
        
        group_by_ports = []
        aggregate_ports = []
        
        for port in trans_elem.findall(".//TRANSFORMFIELD"):
            port_name = port.get("NAME")
            port_type = port.get("PORTTYPE")
            expression = port.get("EXPRESSION", "")
            
            # Check if it's a group by port
            is_group_by = port.get("GROUPBY") == "YES"
            
            if is_group_by:
                group_by_ports.append(port_name)
            
            if expression and any(func in expression.upper() for func in 
                                 ["SUM", "AVG", "COUNT", "MIN", "MAX", "FIRST", "LAST"]):
                aggregate_ports.append({
                    "port_name": port_name,
                    "expression": expression,
                    "datatype": port.get("DATATYPE")
                })
        
        agg_info = {
            "mapping": mapping,
            "transformation": trans_name,
            "group_by_ports": group_by_ports,
            "aggregate_ports": aggregate_ports,
            "pyspark_code": self._generate_aggregator_pyspark(
                group_by_ports, aggregate_ports, trans_name
            )
        }
        
        self.aggregator_logic.append(agg_info)
    
    def _parse_joiner_transformation(self, trans_elem: ET.Element, mapping: str) -> None:
        """Extract and analyze Joiner transformation logic."""
        trans_name = trans_elem.get("NAME")
        join_type = trans_elem.get("JOINTYPE", "NORMAL")
        join_condition = trans_elem.get("JOINCONDITION", "")
        
        # Extract master and detail sources
        master_source = None
        detail_source = None
        
        for attr in trans_elem.findall(".//TABLEATTRIBUTE"):
            if attr.get("NAME") == "Master Source Table":
                master_source = attr.get("VALUE")
            elif attr.get("NAME") == "Detail Source Table":
                detail_source = attr.get("VALUE")
        
        join_info = {
            "mapping": mapping,
            "transformation": trans_name,
            "join_type": self._map_join_type(join_type),
            "join_condition": join_condition,
            "master_source": master_source,
            "detail_source": detail_source,
            "parsed_conditions": self._parse_join_condition(join_condition),
            "pyspark_code": self._generate_joiner_pyspark(
                join_type, join_condition, master_source, detail_source
            )
        }
        
        self.joiner_conditions.append(join_info)
    
    def _parse_filter_transformation(self, trans_elem: ET.Element, mapping: str) -> None:
        """Extract and analyze Filter transformation logic."""
        trans_name = trans_elem.get("NAME")
        filter_condition = trans_elem.get("FILTERCONDITION", "")
        
        filter_info = {
            "mapping": mapping,
            "transformation": trans_name,
            "filter_condition": filter_condition,
            "complexity": self._assess_expression_complexity(filter_condition),
            "pyspark_code": self._generate_filter_pyspark(filter_condition)
        }
        
        self.filter_conditions.append(filter_info)
    
    def _parse_lookup_transformation(self, trans_elem: ET.Element, mapping: str) -> None:
        """Extract and analyze Lookup transformation logic."""
        trans_name = trans_elem.get("NAME")
        lookup_table = trans_elem.get("LOOKUPTABLE", "")
        
        lookup_ports = []
        return_ports = []
        lookup_condition = ""
        caching_strategy = "NONE"
        
        for attr in trans_elem.findall(".//TABLEATTRIBUTE"):
            attr_name = attr.get("NAME")
            attr_value = attr.get("VALUE")
            
            if attr_name == "Lookup Policy on Multiple Match":
                lookup_condition = attr_value
            elif attr_name == "Lookup Caching Enabled":
                caching_strategy = "CACHED" if attr_value == "YES" else "UNCACHED"
            elif attr_name == "Lookup Cache Persistent":
                if attr_value == "YES":
                    caching_strategy = "PERSISTENT"
        
        for port in trans_elem.findall(".//TRANSFORMFIELD"):
            port_name = port.get("NAME")
            port_type = port.get("PORTTYPE")
            
            if port_type == "INPUT/OUTPUT" or port_type == "INPUT":
                lookup_ports.append(port_name)
            elif port_type == "OUTPUT":
                return_ports.append({
                    "name": port_name,
                    "datatype": port.get("DATATYPE")
                })
        
        lookup_info = {
            "mapping": mapping,
            "transformation": trans_name,
            "lookup_table": lookup_table,
            "lookup_ports": lookup_ports,
            "return_ports": return_ports,
            "caching_strategy": caching_strategy,
            "pyspark_code": self._generate_lookup_pyspark(
                lookup_table, lookup_ports, return_ports, caching_strategy
            )
        }
        
        self.lookup_transformations.append(lookup_info)
    
    def _parse_sorter_transformation(self, trans_elem: ET.Element, mapping: str) -> None:
        """Extract and analyze Sorter transformation logic."""
        trans_name = trans_elem.get("NAME")
        
        sort_keys = []
        
        for port in trans_elem.findall(".//TRANSFORMFIELD"):
            sort_key = port.get("SORTKEY")
            if sort_key:
                sort_keys.append({
                    "port_name": port.get("NAME"),
                    "sort_order": "DESC" if port.get("SORTDIRECTION") == "DESCENDING" else "ASC",
                    "sort_key_position": sort_key
                })
        
        # Sort by position
        sort_keys.sort(key=lambda x: int(x.get("sort_key_position", 0)))
        
        sorter_info = {
            "mapping": mapping,
            "transformation": trans_name,
            "sort_keys": sort_keys,
            "pyspark_code": self._generate_sorter_pyspark(sort_keys)
        }
        
        self.sorter_logic.append(sorter_info)
    
    def _parse_router_transformation(self, trans_elem: ET.Element, mapping: str) -> None:
        """Extract and analyze Router transformation logic."""
        trans_name = trans_elem.get("NAME")
        
        router_groups = []
        
        for group in trans_elem.findall(".//ROUTERGROUP"):
            group_name = group.get("NAME")
            group_condition = group.get("CONDITION", "")
            
            router_groups.append({
                "group_name": group_name,
                "condition": group_condition,
                "pyspark_filter": self._convert_expression_to_pyspark(group_condition, "filter")
            })
        
        router_info = {
            "mapping": mapping,
            "transformation": trans_name,
            "router_groups": router_groups,
            "pyspark_code": self._generate_router_pyspark(router_groups)
        }
        
        self.router_logic.append(router_info)
    
    def _parse_union_transformation(self, trans_elem: ET.Element, mapping: str) -> None:
        """Extract and analyze Union transformation logic."""
        trans_name = trans_elem.get("NAME")
        
        union_info = {
            "mapping": mapping,
            "transformation": trans_name,
            "pyspark_code": self._generate_union_pyspark()
        }
        
        self.union_logic.append(union_info)
    
    def _parse_custom_transformation(self, trans_elem: ET.Element, mapping: str) -> None:
        """Extract and analyze custom transformation code."""
        trans_name = trans_elem.get("NAME")
        trans_type = trans_elem.get("TYPE")
        
        custom_code = ""
        language = "UNKNOWN"
        
        for attr in trans_elem.findall(".//TABLEATTRIBUTE"):
            attr_name = attr.get("NAME")
            if attr_name == "Custom Transformation Code":
                custom_code = attr.get("VALUE", "")
            elif attr_name == "Language":
                language = attr.get("VALUE", "")
        
        custom_info = {
            "mapping": mapping,
            "transformation": trans_name,
            "type": trans_type,
            "language": language,
            "custom_code": custom_code,
            "complexity_assessment": self._assess_custom_code_complexity(custom_code),
            "migration_notes": self._generate_custom_migration_notes(custom_code, language)
        }
        
        self.custom_transformations.append(custom_info)
    
    def _assess_expression_complexity(self, expression: str) -> str:
        """Assess complexity of an expression."""
        if not expression:
            return "NONE"
        
        complexity_score = 0
        
        # Check for nested functions
        complexity_score += expression.count("(") * 1
        
        # Check for conditional logic
        if "IIF" in expression.upper() or "DECODE" in expression.upper():
            complexity_score += 3
        
        # Check for string operations
        if any(func in expression.upper() for func in ["SUBSTR", "INSTR", "CONCAT", "REPLACE"]):
            complexity_score += 2
        
        # Check for date operations
        if any(func in expression.upper() for func in ["TO_DATE", "ADD_TO_DATE", "SYSDATE"]):
            complexity_score += 2
        
        if complexity_score == 0:
            return "SIMPLE"
        elif complexity_score < 5:
            return "MODERATE"
        elif complexity_score < 10:
            return "COMPLEX"
        else:
            return "VERY_COMPLEX"
    
    def _identify_expression_pattern(self, expression: str) -> str:
        """Identify reusable patterns in expressions."""
        expr_upper = expression.upper()
        
        if "IIF" in expr_upper or "DECODE" in expr_upper:
            return "CONDITIONAL_LOGIC"
        elif any(func in expr_upper for func in ["SUBSTR", "INSTR", "CONCAT"]):
            return "STRING_MANIPULATION"
        elif any(func in expr_upper for func in ["TO_DATE", "ADD_TO_DATE", "SYSDATE"]):
            return "DATE_OPERATIONS"
        elif any(func in expr_upper for func in ["ROUND", "TRUNC", "ABS"]):
            return "NUMERIC_OPERATIONS"
        elif "LOOKUP" in expr_upper:
            return "LOOKUP_OPERATION"
        else:
            return "GENERAL_TRANSFORMATION"
    
    def _convert_expression_to_pyspark(self, expression: str, port_name: str) -> str:
        """Convert Informatica expression to PySpark equivalent."""
        if not expression:
            return ""
        
        pyspark_expr = expression
        
        # Mapping of Informatica functions to PySpark
        function_mappings = {
            r'\bIIF\s*\(': 'F.when(',
            r'\bTO_DATE\s*\(': 'F.to_date(',
            r'\bTO_CHAR\s*\(': 'F.date_format(',
            r'\bSUBSTR\s*\(': 'F.substring(',
            r'\bINSTR\s*\(': 'F.locate(',
            r'\bCONCAT\s*\(': 'F.concat(',
            r'\bNVL\s*\(': 'F.coalesce(',
            r'\bTRIM\s*\(': 'F.trim(',
            r'\bLTRIM\s*\(': 'F.ltrim(',
            r'\bRTRIM\s*\(': 'F.rtrim(',
            r'\bUPPER\s*\(': 'F.upper(',
            r'\bLOWER\s*\(': 'F.lower(',
            r'\bLENGTH\s*\(': 'F.length(',
            r'\bROUND\s*\(': 'F.round(',
            r'\bTRUNC\s*\(': 'F.trunc(',
            r'\bABS\s*\(': 'F.abs(',
            r'\bSYSDATE': 'F.current_timestamp()',
            r'\|\|': '+',
        }
        
        for infa_func, spark_func in function_mappings.items():
            pyspark_expr = re.sub(infa_func, spark_func, pyspark_expr, flags=re.IGNORECASE)
        
        # Handle IIF to when/otherwise conversion
        pyspark_expr = self._convert_iif_to_when(pyspark_expr)
        
        return f"F.col('{port_name}').alias('{port_name}')  # {pyspark_expr}"
    
    def _convert_iif_to_when(self, expression: str) -> str:
        """Convert IIF statements to PySpark when/otherwise."""
        # This is a simplified conversion - production code would need more robust parsing
        if "IIF" in expression.upper():
            return expression.replace("IIF(", "F.when(").replace(",", ").otherwise(")
        return expression
    
    def _map_join_type(self, infa_join_type: str) -> str:
        """Map Informatica join types to PySpark."""
        join_mapping = {
            "NORMAL": "inner",
            "MASTER OUTER": "left_outer",
            "DETAIL OUTER": "right_outer",
            "FULL OUTER": "outer"
        }
        return join_mapping.get(infa_join_type.upper(), "inner")
    
    def _parse_join_condition(self, condition: str) -> List[Dict[str, str]]:
        """Parse join condition into structured format."""
        conditions = []
        if not condition:
            return conditions
        
        # Split on AND
        parts = re.split(r'\s+AND\s+', condition, flags=re.IGNORECASE)
        
        for part in parts:
            # Extract left and right columns
            match = re.match(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', part.strip())
            if match:
                conditions.append({
                    "left_table": match.group(1),
                    "left_column": match.group(2),
                    "right_table": match.group(3),
                    "right_column": match.group(4)
                })
        
        return conditions
    
    def _generate_aggregator_pyspark(self, group_by_ports: List[str], 
                                    aggregate_ports: List[Dict], trans_name: str) -> str:
        """Generate PySpark code for Aggregator transformation."""
        code_lines = [
            f"# Aggregator: {trans_name}",
            f"df_{trans_name} = df.groupBy(",
        ]
        
        if group_by_ports:
            group_cols = ", ".join([f"F.col('{col}')" for col in group_by_ports])
            code_lines.append(f"    {group_cols}")
        
        code_lines.append(").agg(")
        
        agg_expressions = []
        for agg_port in aggregate_ports:
            expr = agg_port['expression'].upper()
            port_name = agg_port['port_name']
            
            if "SUM(" in expr:
                col = self._extract_column_from_agg(expr, "SUM")
                agg_expressions.append(f"    F.sum(F.col('{col}')).alias('{port_name}')")
            elif "AVG(" in expr:
                col = self._extract_column_from_agg(expr, "AVG")
                agg_expressions.append(f"    F.avg(F.col('{col}')).alias('{port_name}')")
            elif "COUNT(" in expr:
                col = self._extract_column_from_agg(expr, "COUNT")
                agg_expressions.append(f"    F.count(F.col('{col}')).alias('{port_name}')")
            elif "MIN(" in expr:
                col = self._extract_column_from_agg(expr, "MIN")
                agg_expressions.append(f"    F.min(F.col('{col}')).alias('{port_name}')")
            elif "MAX(" in expr:
                col = self._extract_column_from_agg(expr, "MAX")
                agg_expressions.append(f"    F.max(F.col('{col}')).alias('{port_name}')")
        
        code_lines.append(",\n".join(agg_expressions))
        code_lines.append(")")
        
        return "\n".join(code_lines)
    
    def _extract_column_from_agg(self, expression: str, function: str) -> str:
        """Extract column name from aggregate function."""
        pattern = rf'{function}\s*\(\s*(\w+)\s*\)'
        match = re.search(pattern, expression, re.IGNORECASE)
        return match.group(1) if match else "unknown_column"
    
    def _generate_joiner_pyspark(self, join_type: str, condition: str, 
                                master: str, detail: str) -> str:
        """Generate PySpark code for Joiner transformation."""
        spark_join_type = self._map_join_type(join_type)
        parsed_conditions = self._parse_join_condition(condition)
        
        code_lines = [
            f"# Joiner: {master} and {detail}",
            f"df_joined = df_{master}.join(",
            f"    df_{detail},",
        ]
        
        if parsed_conditions:
            join_conditions = []
            for cond in parsed_conditions:
                join_conditions.append(
                    f"(df_{master}['{cond['left_column']}'] == df_{detail}['{cond['right_column']}'])"
                )
            
            code_lines.append(f"    {' & '.join(join_conditions)},")
        
        code_lines.append(f"    how='{spark_join_type}'")
        code_lines.append(")")
        
        return "\n".join(code_lines)
    
    def _generate_filter_pyspark(self, condition: str) -> str:
        """Generate PySpark code for Filter transformation."""
        pyspark_condition = self._convert_expression_to_pyspark(condition, "filter")
        
        return f"""# Filter transformation
df_filtered = df.filter({pyspark_condition})"""
    
    def _generate_lookup_pyspark(self, lookup_table: str, lookup_ports: List[str], 
                                return_ports: List[Dict], caching: str) -> str:
        """Generate PySpark code for Lookup transformation."""
        code_lines = [
            f"# Lookup: {lookup_table}",
            f"df_lookup = spark.table('{lookup_table}')",
        ]
        
        if caching == "CACHED" or caching == "PERSISTENT":
            code_lines.append("df_lookup.cache()")
        
        if lookup_ports:
            join_conditions = [f"df['{port}'] == df_lookup['{port}']" for port in lookup_ports]
            code_lines.append(
                f"\ndf_with_lookup = df.join(\n"
                f"    df_lookup,\n"
                f"    {' & '.join(join_conditions)},\n"
                f"    how='left_outer'\n"
                f")"
            )
        
        return "\n".join(code_lines)
    
    def _generate_sorter_pyspark(self, sort_keys: List[Dict]) -> str:
        """Generate PySpark code for Sorter transformation."""
        if not sort_keys:
            return "# No sort keys defined"
        
        sort_expressions = []
        for key in sort_keys:
            if key['sort_order'] == 'DESC':
                sort_expressions.append(f"F.col('{key['port_name']}').desc()")
            else:
                sort_expressions.append(f"F.col('{key['port_name']}').asc()")
        
        return f"""# Sorter transformation
df_sorted = df.orderBy({', '.join(sort_expressions)})"""
    
    def _generate_router_pyspark(self, router_groups: List[Dict]) -> str:
        """Generate PySpark code for Router transformation."""
        code_lines = ["# Router transformation - Create multiple output DataFrames"]
        
        for group in router_groups:
            group_name = group['group_name'].replace(" ", "_")
            condition = group['pyspark_filter']
            code_lines.append(f"df_{group_name} = df.filter({condition})")
        
        # Add default group
        all_conditions = " | ".join([f"({g['pyspark_filter']})" for g in router_groups])
        code_lines.append(f"df_default = df.filter(~({all_conditions}))")
        
        return "\n".join(code_lines)
    
    def _generate_union_pyspark(self) -> str:
        """Generate PySpark code for Union transformation."""
        return """# Union transformation
df_union = df1.unionByName(df2, allowMissingColumns=True)
# For multiple DataFrames:
# from functools import reduce
# df_union = reduce(DataFrame.unionByName, [df1, df2, df3], allowMissingColumns=True)"""
    
    def _assess_custom_code_complexity(self, code: str) -> Dict[str, Any]:
        """Assess complexity of custom transformation code."""
        if not code:
            return {"complexity": "NONE", "lines": 0}
        
        lines = code.count("\n") + 1
        
        complexity_indicators = {
            "loops": len(re.findall(r'\b(for|while)\b', code, re.IGNORECASE)),
            "conditionals": len(re.findall(r'\b(if|case|switch)\b', code, re.IGNORECASE)),
            "function_calls": len(re.findall(r'\w+\s*\(', code)),
            "sql_queries": len(re.findall(r'\b(SELECT|INSERT|UPDATE|DELETE)\b', code, re.IGNORECASE))
        }
        
        complexity_score = (
            lines * 0.1 +
            complexity_indicators["loops"] * 3 +
            complexity_indicators["conditionals"] * 2 +
            complexity_indicators["function_calls"] * 0.5 +
            complexity_indicators["sql_queries"] * 5
        )
        
        if complexity_score < 10:
            complexity = "LOW"
        elif complexity_score < 30:
            complexity = "MEDIUM"
        elif complexity_score < 60:
            complexity = "HIGH"
        else:
            complexity = "VERY_HIGH"
        
        return {
            "complexity": complexity,
            "lines": lines,
            "complexity_score": complexity_score,
            "indicators": complexity_indicators
        }
    
    def _generate_custom_migration_notes(self, code: str, language: str) -> str:
        """Generate migration notes for custom transformations."""
        notes = []
        
        if not code:
            return "No custom code to migrate"
        
        notes.append(f"Original Language: {language}")
        
        if language.upper() in ["C", "C++"]:
            notes.append("Consider reimplementing in Python or using PySpark UDFs")
            notes.append("Review performance implications of UDFs")
        
        if "SQL" in code.upper():
            notes.append("Embedded SQL detected - convert to DataFrame operations where possible")
        
        if any(func in code.upper() for func in ["CURSOR", "FETCH", "OPEN"]):
            notes.append("Cursor-based logic detected - refactor to set-based operations")
        
        return " | ".join(notes)
    
    def generate_transformation_catalog(self) -> Dict[str, Any]:
        """Generate comprehensive transformation catalog with counts and examples."""
        catalog = {
            "summary": {
                "total_transformations": sum(len(v) for v in self.transformation_catalog.values()),
                "transformation_types": {k: len(v) for k, v in self.transformation_catalog.items()}
            },
            "expression_transformations": {
                "count": len(self.expression_formulas),
                "complexity_distribution": Counter([e['complexity'] for e in self.expression_formulas]),
                "pattern_distribution": {k: len(v) for k, v in self.transformation_patterns.items()},
                "examples": self.expression_formulas[:5]
            },
            "aggregator_transformations": {
                "count": len(self.aggregator_logic),
                "examples": self.aggregator_logic[:3]
            },
            "joiner_transformations": {
                "count": len(self.joiner_conditions),
                "join_type_distribution": Counter([j['join_type'] for j in self.joiner_conditions]),
                "examples": self.joiner_conditions[:3]
            },
            "filter_transformations": {
                "count": len(self.filter_conditions),
                "complexity_distribution": Counter([f['complexity'] for f in self.filter_conditions]),
                "examples": self.filter_conditions[:3]
            },
            "lookup_transformations": {
                "count