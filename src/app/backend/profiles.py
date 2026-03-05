"""User profile definitions and data-scoping logic for the Meridian Portal.

Each profile represents a demo persona. The profile switcher in the frontend
selects one of these; the backend uses the profile to determine which schemas
and tables the current user can access, and which Genie space to embed.
"""

from dataclasses import dataclass, field


@dataclass
class Profile:
    id: str
    name: str
    role: str
    persona: str
    business_unit: str
    avatar_initials: str
    allowed_schemas: list[str]
    genie_space_id: str | None
    nav_tabs: list[str] = field(default_factory=list)
    subscription_tier: str | None = None


PROFILES: dict[str, Profile] = {
    "sarah": Profile(
        id="sarah",
        name="Sarah Chen",
        role="RevOps Analyst",
        persona="Internal",
        business_unit="internal",
        avatar_initials="SC",
        allowed_schemas=["internal", "regulatory", "research"],
        genie_space_id=None,  # Set after Genie space creation
        nav_tabs=["Sales Dashboard", "Product Usage", "Genie"],
    ),
    "james": Profile(
        id="james",
        name="James Rivera",
        role="Data Engineering Lead, Acme Bank",
        persona="External Customer (Regulatory)",
        business_unit="regulatory",
        avatar_initials="JR",
        allowed_schemas=["regulatory"],
        genie_space_id=None,  # TODO Phase 2: Set after Regulatory Genie creation
        nav_tabs=["Data Catalog", "Genie", "Connect Your Environment"],
        subscription_tier="sec_only",
    ),
    "anika": Profile(
        id="anika",
        name="Dr. Anika Park",
        role="Research Director, NIH",
        persona="External Customer (Research)",
        business_unit="research",
        avatar_initials="AP",
        allowed_schemas=["research"],
        genie_space_id=None,  # Set after Research Genie creation
        nav_tabs=["Research Q&A", "Paper Browser", "Citation Explorer"],
    ),
}


def get_profile(profile_id: str) -> Profile:
    if profile_id not in PROFILES:
        raise ValueError(f"Unknown profile: {profile_id}. Valid: {list(PROFILES.keys())}")
    return PROFILES[profile_id]


def get_all_profiles() -> list[dict]:
    """Return serializable list of all profiles for the frontend."""
    return [
        {
            "id": p.id,
            "name": p.name,
            "role": p.role,
            "persona": p.persona,
            "avatar_initials": p.avatar_initials,
            "nav_tabs": p.nav_tabs,
            "business_unit": p.business_unit,
        }
        for p in PROFILES.values()
    ]


def get_schema_filter(profile: Profile) -> str:
    """Return a SQL-safe schema filter clause for the profile's allowed schemas."""
    schemas = ", ".join(f"'{s}'" for s in profile.allowed_schemas)
    return f"schema_name IN ({schemas})"
