import json
import logging
import os
from typing import Dict, Any, Optional
import boto3
from jsonschema import validate, ValidationError, Draft7Validator
from botocore.exceptions import ClientError
# Configure logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

# Initialize AWS clients outside handler for connection reuse
eventbridge_client = boto3.client('events')
schemas_client = boto3.client('schemas')

EVENT_SOURCE = "orcabus.executionhandler"
DETAIL_TYPE = "WorkflowRunUpdate"
EVENT_BUS_NAME = os.environ.get('EVENT_BUS_NAME', 'default')

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler that validates JSON payload and forwards to EventBridge

    Args:
        event: Lambda event containing the JSON payload to validate
        context: Lambda context object

    Returns:
        Dict with statusCode and validation result
    """
    try:
        logger.info(f"Processing event: {json.dumps(event, default=str)}")

        if isinstance(event, str):
            event = json.loads(event)

        # Extract payload from event
        logger.info("Extracting payload from event")
        payload = extract_payload(event)
        if not payload:
            return create_error_response(400, "No payload found in event")

        # Load and validate schema
        logger.info("Loading validation schema")
        schema = load_schema()
        if not schema:
            return create_error_response(500, "Failed to load validation schema")

        # Validate payload against schema
        logger.info("Validating payload against schema")
        validation_result = validate_payload(payload, schema)
        if not validation_result['valid']:
            logger.warning(f"Validation failed: {validation_result['errors']}")
            return create_error_response(400, "Payload validation failed", validation_result['errors'])

        # Run additional custom validations if needed
        # (e.g., cross-field checks, business logic validations)
        # - if referenced Workflow is valid
        # - if worflow run name is valid (expected format for manual runs)
        # - if portal run id is vaid ULID
        # - if status is 'DRAFT'
        # - if

        # Forward to EventBridge
        logger.info("Forwarding validated payload to EventBridge")
        event_result = send_to_eventbridge(payload)
        if not event_result['success']:
            logger.error(f"EventBridge send failed: {event_result['error']}")
            return create_error_response(500, "Failed to send event to EventBridge", event_result['error'])

        logger.info(f"Successfully processed and forwarded event: {event_result['event_id']}")
        return create_success_response({
            'message': 'Event validated and forwarded successfully',
            'event_id': event_result['event_id']
        })

    except Exception as e:
        logger.error(f"Unhandled error: {str(e)}", exc_info=True)
        return create_error_response(500, "Internal server error")

def extract_payload(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract JSON payload from various event sources"""
    logger.info(f"Extracting payload from {event}")

    try:
        # Direct invocation with payload
        if 'payload' in event:
            logger.info("Payload found in 'payload' key")
            return event['payload']

        # API Gateway event
        if 'body' in event:
            logger.info("Payload found in 'body' key")
            body = event['body']
            if isinstance(body, str):
                return json.loads(body)
            return body

        # EventBridge or direct JSON
        if 'detail' in event and 'detail-type' in event:
            logger.info("Payload found in EventBridge 'detail' key")
            return event['detail']

        if 'Detail' in event and 'DetailType' in event:
            logger.info("Payload found in EventBridge 'Detail' key (boto version)")
            return event['Detail']

        # Assume the entire event is the payload
        logger.info("Using entire event as payload")
        return event

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON payload: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error extracting payload: {str(e)}")
        return None

def load_schema() -> Optional[Dict[str, Any]]:
    """Load JSON schema from EventBridge Schema Registry"""
    try:
        # Get schema configuration from environment variables
        schema_name = os.environ.get('SCHEMA_NAME', 'orcabus.workflowmanager@WorkflowRunUpdate')
        registry_name = os.environ.get('SCHEMA_REGISTRY_NAME', 'discovered-schemas')

        logger.info(f"Loading schema '{schema_name}' from registry '{registry_name}'")

        # Fetch schema from EventBridge Schema Registry
        schema_content = fetch_schema_from_registry(schema_name, registry_name)
        if schema_content:
            return schema_content

        logger.warning("Schema not found in registry, trying fallback!")
        # Fallback: Try to load from environment variable
        schema_json = os.environ.get('VALIDATION_SCHEMA')
        if schema_json:
            logger.info("Using schema from environment variable")
            return json.loads(schema_json)

        # Fallback: Try to load from file
        schema_file = os.environ.get('SCHEMA_FILE_PATH', 'schema.json')
        try:
            logger.info(f"Attempting to load schema from file: {schema_file}")
            with open(schema_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Schema file not found: {schema_file}")

        # Last resort: Default schema
        logger.warning("Using default schema as fallback")
        return get_default_schema()

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in schema: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error loading schema: {str(e)}")
        return None

def fetch_schema_from_registry(schema_name: str, registry_name: str) -> Optional[Dict[str, Any]]:
    """Fetch schema from AWS EventBridge Schema Registry"""
    try:
        # Get the latest version of the schema
        response = schemas_client.describe_schema(
            RegistryName=registry_name,
            SchemaName=schema_name
        )

        schema_content = response.get('Content')
        if not schema_content:
            logger.error(f"No content found for schema '{schema_name}'")
            return None

        # Parse the schema content
        # EventBridge schemas can be in different formats (OpenAPI, JSONSchema)
        schema_type = response.get('Type', 'JSONSchemaDraft4')

        if schema_type in ['JSONSchemaDraft4', 'JSONSchemaDraft7']:
            # Direct JSON Schema
            return json.loads(schema_content)
        elif schema_type == 'OpenApi3':
            # Extract JSON Schema from OpenAPI spec
            return extract_schema_from_openapi(schema_content)
        else:
            logger.error(f"Unsupported schema type: {schema_type}")
            return None

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']

        if error_code == 'NotFoundException':
            logger.error(f"Schema '{schema_name}' not found in registry '{registry_name}'")
        elif error_code == 'ForbiddenException':
            logger.error(f"Access denied to schema '{schema_name}' in registry '{registry_name}'")
        else:
            logger.error(f"AWS Schemas API error ({error_code}): {error_message}")

        return None
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse schema content as JSON: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching schema from registry: {str(e)}")
        return None

def extract_schema_from_openapi(openapi_content: str) -> Optional[Dict[str, Any]]:
    """Extract JSON Schema from OpenAPI specification"""
    try:
        openapi_spec = json.loads(openapi_content)

        # Look for the main event schema in components/schemas
        components = openapi_spec.get('components', {})
        schemas = components.get('schemas', {})

        # Try to find the main event schema
        # Common patterns: Event, WorkflowRunUpdate, etc.
        main_schema_keys = ['Event', 'WorkflowRunUpdate', 'AWSEvent']

        for key in main_schema_keys:
            if key in schemas:
                schema = schemas[key]
                # Convert OpenAPI schema to JSON Schema if needed
                return convert_openapi_to_jsonschema(schema)

        # If no main schema found, try the first schema
        if schemas:
            first_schema_key = next(iter(schemas))
            logger.info(f"Using first available schema: {first_schema_key}")
            return convert_openapi_to_jsonschema(schemas[first_schema_key])

        logger.error("No schemas found in OpenAPI specification")
        return None

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse OpenAPI content: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error extracting schema from OpenAPI: {str(e)}")
        return None

def convert_openapi_to_jsonschema(openapi_schema: Dict[str, Any]) -> Dict[str, Any]:
    """Convert OpenAPI schema format to JSON Schema format"""
    # Most OpenAPI schemas are already compatible with JSON Schema
    # Add JSON Schema specific fields if missing
    if '$schema' not in openapi_schema:
        openapi_schema['$schema'] = 'http://json-schema.org/draft-07/schema#'

    return openapi_schema

def get_default_schema() -> Dict[str, Any]:
    """Return a default schema for WorkflowRunUpdate validation"""
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "WorkflowRunUpdate Event Schema",
        "type": "object",
        "properties": {
            "version": {
                "type": "string"
            },
            "id": {
                "type": "string"
            },
            "detail-type": {
                "type": "string"
            },
            "source": {
                "type": "string"
            },
            "account": {
                "type": "string"
            },
            "time": {
                "type": "string",
                "format": "date-time"
            },
            "region": {
                "type": "string"
            },
            "detail": {
                "type": "object",
                "properties": {
                    "workflowRunId": {
                        "type": "string"
                    },
                    "status": {
                        "type": "string",
                        "enum": ["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"]
                    },
                    "timestamp": {
                        "type": "string",
                        "format": "date-time"
                    }
                },
                "required": ["workflowRunId", "status"],
                "additionalProperties": True
            }
        },
        "required": ["version", "id", "detail-type", "source", "detail"],
        "additionalProperties": True
    }

def validate_payload(payload: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
    """Validate payload against JSON schema"""
    # Pack the payload into an EventBridge conform Event structure as the schema expects it
    event = {
        "source": "orcabus.event.validation",
        "detail-type": "WorkflowRunUpdate",
        "detail": payload
    }
    try:
        # Use Draft7Validator for better error reporting
        validator = Draft7Validator(schema)
        errors = list(validator.iter_errors(event))

        if errors:
            error_messages = []
            for error in errors:
                path = " -> ".join(str(p) for p in error.absolute_path) if error.absolute_path else "root"
                error_messages.append(f"{path}: {error.message}")

            logger.info("Payload validation failed")
            return {
                'valid': False,
                'errors': error_messages
            }

        logger.info("Payload validation successful")
        return {'valid': True, 'errors': []}

    except Exception as e:
        logger.error(f"Schema validation error: {str(e)}")
        return {
            'valid': False,
            'errors': [f"Validation process failed: {str(e)}"]
        }

def send_to_eventbridge(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Send validated payload to EventBridge"""
    try:
        logger.info(f"Using event bus: {EVENT_BUS_NAME}")

        # Prepare EventBridge event
        event_entry = {
            'Source': EVENT_SOURCE,
            'DetailType': DETAIL_TYPE,
            'Detail': json.dumps(payload),
            'EventBusName': EVENT_BUS_NAME
        }

        # Add optional fields if present in payload
        if 'resources' in payload:
            event_entry['Resources'] = payload['resources']

        # Send to EventBridge
        logger.info(f"Emitting event: {event_entry}")
        # response = eventbridge_client.put_events(Entries=[event_entry])
        # TODO: remove after debugging
        return {
            'success': True,
            'event_id': 'debug-event-id'
        }

        # Check for failures
        if response['FailedEntryCount'] > 0:
            failed_entries = response.get('Entries', [])
            errors = [entry.get('ErrorMessage', 'Unknown error') for entry in failed_entries if 'ErrorCode' in entry]
            return {
                'success': False,
                'error': f"EventBridge put_events failed: {', '.join(errors)}"
            }

        # Get event ID from response
        event_id = response['Entries'][0].get('EventId', 'unknown')

        return {
            'success': True,
            'event_id': event_id
        }

    except ClientError as e:
        error_msg = e.response['Error']['Message']
        return {
            'success': False,
            'error': f"AWS EventBridge error: {error_msg}"
        }
    except Exception as e:
        return {
            'success': False,
            'error': f"Unexpected error sending to EventBridge: {str(e)}"
        }

def create_success_response(data: Dict[str, Any]) -> Dict[str, Any]:
    """Create standardized success response"""
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({
            'success': True,
            **data
        })
    }

def create_error_response(status_code: int, message: str, details: Any = None) -> Dict[str, Any]:
    """Create standardized error response"""
    response_body = {
        'success': False,
        'error': message
    }

    if details:
        response_body['details'] = details

    return {
        'statusCode': status_code,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(response_body)
    }
