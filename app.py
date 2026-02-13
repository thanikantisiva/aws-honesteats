"""
AWS Lambda handler for rork-honesteats API
Uses AWS Lambda Power Tools for API Gateway integration
"""

from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.event_handler import APIGatewayRestResolver, CORSConfig
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.middleware_factory import lambda_handler_decorator

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


@middleware_handler
@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """
    Main Lambda handler function
    """
    return app.resolve(event, context)
