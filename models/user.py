"""User model"""
from typing import Optional
from datetime import datetime


class User:
    """Unified User model for both customers and riders"""
    
    # Rider status constants
    RIDER_STATUS_SIGNUP_DONE = "SIGNUP_DONE"
    RIDER_STATUS_APPROVED = "APPROVED"
    RIDER_STATUS_REJECTED = "REJECTED"
    
    def __init__(
        self,
        phone: str,
        name: str = None,
        email: Optional[str] = None,
        role: str = "CUSTOMER",
        is_active: bool = True,
        created_at: Optional[str] = None,
        date_of_birth: Optional[str] = None,
        fcm_token: Optional[str] = None,
        fcm_token_updated_at: Optional[str] = None,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        geohash: Optional[str] = None,
        # Rider-specific fields (only when role="RIDER")
        rider_id: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        address: Optional[str] = None,
        aadhar_number: Optional[str] = None,
        aadhar_image_base64: Optional[str] = None,  # Deprecated, kept for backward compatibility
        aadhar_image_url: Optional[str] = None,  # S3 URL (new)
        pan_number: Optional[str] = None,
        pan_image_base64: Optional[str] = None,  # Deprecated, kept for backward compatibility
        pan_image_url: Optional[str] = None,  # S3 URL (new)
        rider_status: Optional[str] = None,
        rejection_reason: Optional[str] = None,
        approved_at: Optional[str] = None
    ):
        self.phone = phone
        self.name = name  # Used for customers
        self.email = email
        self.role = role
        self.is_active = is_active
        self.created_at = created_at or datetime.utcnow().isoformat()
        self.date_of_birth = date_of_birth
        self.fcm_token = fcm_token
        self.fcm_token_updated_at = fcm_token_updated_at
        self.lat = lat
        self.lng = lng
        self.geohash = geohash
        
        # Rider-specific fields
        self.rider_id = rider_id
        self.first_name = first_name
        self.last_name = last_name
        self.address = address
        self.aadhar_number = aadhar_number
        self.aadhar_image_base64 = aadhar_image_base64  # Deprecated
        self.aadhar_image_url = aadhar_image_url  # New S3 URL
        self.pan_number = pan_number
        self.pan_image_base64 = pan_image_base64  # Deprecated
        self.pan_image_url = pan_image_url  # New S3 URL
        self.rider_status = rider_status
        self.rejection_reason = rejection_reason
        self.approved_at = approved_at
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        result = {
            "phone": self.phone,
            "role": self.role,
            "isActive": self.is_active,
            "createdAt": self.created_at
        }
        
        # Customer fields
        if self.name:
            result["name"] = self.name
        if self.email:
            result["email"] = self.email
        if self.date_of_birth:
            result["dateOfBirth"] = self.date_of_birth
        if self.fcm_token:
            result["fcmToken"] = self.fcm_token
        if self.fcm_token_updated_at:
            result["fcmTokenUpdatedAt"] = self.fcm_token_updated_at
        if self.lat is not None:
            result["lat"] = self.lat
        if self.lng is not None:
            result["lng"] = self.lng
        if self.geohash:
            result["geohash"] = self.geohash
        
        # Rider fields
        if self.rider_id:
            result["riderId"] = self.rider_id
        if self.first_name:
            result["firstName"] = self.first_name
        if self.last_name:
            result["lastName"] = self.last_name
        if self.address:
            result["address"] = self.address
        if self.aadhar_number:
            result["aadharNumber"] = self.aadhar_number
        if self.aadhar_image_base64:
            result["aadharImageBase64"] = self.aadhar_image_base64  # Deprecated
        if self.aadhar_image_url:
            result["aadharImageUrl"] = self.aadhar_image_url  # New
        if self.pan_number:
            result["panNumber"] = self.pan_number
        if self.pan_image_base64:
            result["panImageBase64"] = self.pan_image_base64  # Deprecated
        if self.pan_image_url:
            result["panImageUrl"] = self.pan_image_url  # New
        if self.rider_status:
            result["riderStatus"] = self.rider_status
        if self.rejection_reason:
            result["rejectionReason"] = self.rejection_reason
        if self.approved_at:
            result["approvedAt"] = self.approved_at
            
        return result
    
    @classmethod
    def from_dynamodb_item(cls, item: dict) -> "User":
        """Create User from DynamoDB item"""
        return cls(
            phone=item.get("phone", {}).get("S", ""),
            name=item.get("name", {}).get("S") if "name" in item else None,
            email=item.get("email", {}).get("S") if "email" in item else None,
            role=item.get("role", {}).get("S", "CUSTOMER"),
            is_active=item.get("isActive", {}).get("BOOL", True) if "isActive" in item else True,
            created_at=item.get("createdAt", {}).get("S", ""),
            date_of_birth=item.get("dateOfBirth", {}).get("S") if "dateOfBirth" in item else None,
            fcm_token=item.get("fcmToken", {}).get("S") if "fcmToken" in item else None,
            fcm_token_updated_at=item.get("fcmTokenUpdatedAt", {}).get("S") if "fcmTokenUpdatedAt" in item else None,
            lat=float(item.get("lat", {}).get("N")) if "lat" in item else None,
            lng=float(item.get("lng", {}).get("N")) if "lng" in item else None,
            geohash=item.get("geohash", {}).get("S") if "geohash" in item else None,
            # Rider fields
            rider_id=item.get("riderId", {}).get("S") if "riderId" in item else None,
            first_name=item.get("firstName", {}).get("S") if "firstName" in item else None,
            last_name=item.get("lastName", {}).get("S") if "lastName" in item else None,
            address=item.get("address", {}).get("S") if "address" in item else None,
            aadhar_number=item.get("aadharNumber", {}).get("S") if "aadharNumber" in item else None,
            aadhar_image_base64=item.get("aadharImageBase64", {}).get("S") if "aadharImageBase64" in item else None,
            aadhar_image_url=item.get("aadharImageUrl", {}).get("S") if "aadharImageUrl" in item else None,
            pan_number=item.get("panNumber", {}).get("S") if "panNumber" in item else None,
            pan_image_base64=item.get("panImageBase64", {}).get("S") if "panImageBase64" in item else None,
            pan_image_url=item.get("panImageUrl", {}).get("S") if "panImageUrl" in item else None,
            rider_status=item.get("riderStatus", {}).get("S") if "riderStatus" in item else None,
            rejection_reason=item.get("rejectionReason", {}).get("S") if "rejectionReason" in item else None,
            approved_at=item.get("approvedAt", {}).get("S") if "approvedAt" in item else None
        )
    
    def to_dynamodb_item(self) -> dict:
        """Convert to DynamoDB item format"""
        item = {
            "phone": {"S": self.phone},
            "role": {"S": self.role},
            "isActive": {"BOOL": self.is_active},
            "createdAt": {"S": self.created_at}
        }
        
        # Customer fields
        if self.name:
            item["name"] = {"S": self.name}
        if self.email:
            item["email"] = {"S": self.email}
        if self.date_of_birth:
            item["dateOfBirth"] = {"S": self.date_of_birth}
        if self.fcm_token:
            item["fcmToken"] = {"S": self.fcm_token}
        if self.fcm_token_updated_at:
            item["fcmTokenUpdatedAt"] = {"S": self.fcm_token_updated_at}
        if self.lat is not None:
            item["lat"] = {"N": str(self.lat)}
        if self.lng is not None:
            item["lng"] = {"N": str(self.lng)}
        if self.geohash:
            item["geohash"] = {"S": self.geohash}
        
        # Rider fields
        if self.rider_id:
            item["riderId"] = {"S": self.rider_id}
        if self.first_name:
            item["firstName"] = {"S": self.first_name}
        if self.last_name:
            item["lastName"] = {"S": self.last_name}
        if self.address:
            item["address"] = {"S": self.address}
        if self.aadhar_number:
            item["aadharNumber"] = {"S": self.aadhar_number}
        if self.aadhar_image_base64:
            item["aadharImageBase64"] = {"S": self.aadhar_image_base64}
        if self.aadhar_image_url:
            item["aadharImageUrl"] = {"S": self.aadhar_image_url}
        if self.pan_number:
            item["panNumber"] = {"S": self.pan_number}
        if self.pan_image_base64:
            item["panImageBase64"] = {"S": self.pan_image_base64}
        if self.pan_image_url:
            item["panImageUrl"] = {"S": self.pan_image_url}
        if self.rider_status:
            item["riderStatus"] = {"S": self.rider_status}
        if self.rejection_reason:
            item["rejectionReason"] = {"S": self.rejection_reason}
        if self.approved_at:
            item["approvedAt"] = {"S": self.approved_at}
            
        return item
