"""
AWS Lambda handler for rork-honesteats API
Uses AWS Lambda Power Tools for API Gateway integration
"""

from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.event_handler import APIGatewayRestResolver, CORSConfig
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.middleware_factory import lambda_handler_decorator
from middleware.jwt_auth import verify_token

# Import route handlers
from routes.auth_routes import register_auth_routes
from routes.user_routes import register_user_routes
from routes.restaurant_routes import register_restaurant_routes
from routes.menu_routes import register_menu_routes
from routes.order_routes import register_order_routes
from routes.rider_routes import register_rider_routes
from routes.rider_signup_routes import register_rider_signup_routes
from routes.rider_order_routes import register_rider_order_routes
from routes.earnings_routes import register_earnings_routes
from routes.address_routes import register_address_routes
from routes.location_routes import register_location_routes
from routes.payment_routes import register_payment_routes
from routes.delivery_routes import register_delivery_routes
from routes.image_routes import register_image_routes
from routes.coupon_routes import register_coupon_routes

# Initialize AWS Lambda Power Tools
logger = Logger(service="rork-honesteats-api")
tracer = Tracer(service="rork-honesteats-api")
metrics = Metrics(namespace="RorkHonestEats", service="api")

# Define public routes that don't require JWT authentication
PUBLIC_ROUTES = [
    "/api/v1/auth/send-otp",
    "/api/v1/auth/verify-otp",
    "/api/v1/riders/login/check",
    "/api/v1/riders/signup",
    "/api/v1/riders/documents/upload",
    "/health",
    "/api/v1/status",
]

# Create API Gateway resolver with CORS enabled
app = APIGatewayRestResolver(
    cors=CORSConfig(
        allow_origin="*",  # Allows all origins (use specific domain in production)
        extra_origins=["http://localhost:4200", "http://localhost:3000"],
        max_age=300,
        expose_headers=["Content-Type", "Authorization"],
        allow_headers=["Content-Type", "Authorization", "X-Api-Key"]
    )
)

# Register all routes
register_auth_routes(app)
register_user_routes(app)
register_restaurant_routes(app)
register_menu_routes(app)
register_order_routes(app)
register_rider_routes(app)
register_rider_signup_routes(app)
register_rider_order_routes(app)
register_earnings_routes(app)
register_address_routes(app)
register_location_routes(app)
register_payment_routes(app)
register_delivery_routes(app)
register_image_routes(app)
register_coupon_routes(app)


@app.get("/health")
@tracer.capture_method
def get_health():
    """Health check endpoint"""
    logger.info("Health check requested")
    metrics.add_metric(name="HealthCheck", unit="Count", value=1)
    return {"status": "healthy", "service": "rork-honesteats-api"}


@app.get("/api/v1/status")
@tracer.capture_method
def get_status():
    """Get service status"""
    logger.info("Status check requested")
    return {
        "status": "operational",
        "version": "1.0.0",
        "service": "rork-honesteats-api"
    }


@lambda_handler_decorator
def middleware_handler(handler, event, context):
    """Middleware for logging and error handling"""
    logger.info("Lambda invocation started")
    
    try:
        response = handler(event, context)
        logger.info("Lambda invocation completed successfully")
        return response
    except Exception as e:
        logger.error("Lambda invocation failed", exc_info=True)
        raise


@lambda_handler_decorator
def auth_middleware(handler, event, context):
    """Global authentication middleware - checks JWT for all routes except public ones"""
    
    # Get the path from the event
    path = event.get('path', '')
    method = event.get('httpMethod', '')
    
    # Check if this is a public route
    is_public = path in PUBLIC_ROUTES
    
    if not is_public:
        # Get Authorization header
        headers = event.get('headers', {})
        auth_header = headers.get('authorization') or headers.get('Authorization')
        
        if not auth_header:
            logger.warning(f"Missing Authorization header for {method} {path}")
            return {
                'statusCode': 401,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Api-Key',
                },
                'body': '{"error": "Unauthorized", "message": "Missing authentication token"}'
            }
        
        # Check Bearer token format
        if not auth_header.startswith('Bearer '):
            logger.warning(f"Invalid Authorization header format for {method} {path}")
            return {
                'statusCode': 401,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Api-Key',
                },
                'body': '{"error": "Unauthorized", "message": "Invalid token format"}'
            }
        
        # Extract and verify token
        token = auth_header.replace('Bearer ', '').strip()
        payload = verify_token(token)
        
        if not payload:
            logger.warning(f"Token verification failed for {method} {path}")
            return {
                'statusCode': 401,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Api-Key',
                },
                'body': '{"error": "Unauthorized", "message": "Invalid or expired token"}'
            }
        
        logger.info(f"✅ Authenticated request from: {payload.get('phone', '')[:5]}*** for {method} {path}")
    
    # Continue to the actual handler
    return handler(event, context)


@auth_middleware
@middleware_handler
@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """
    Main Lambda handler function
    """
    return app.resolve(event, context)
