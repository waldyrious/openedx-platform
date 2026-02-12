"""
Toggles for user_authn
"""


from django.conf import settings
from edx_toggles.toggles import WaffleFlag

from openedx.core.djangoapps.site_configuration import helpers as configuration_helpers
from openedx.core.djangoapps.theming.helpers import get_current_request

# Namespace for user authentication toggles
WAFFLE_FLAG_NAMESPACE = 'user_authn'

# .. toggle_name: user_authn.enable_enterprise_redirect_to_authn
# .. toggle_implementation: WaffleFlag
# .. toggle_default: False
# .. toggle_description: When enabled, Enterprise (B2B) users are redirected to the AuthN MFE like B2C users.
# .. toggle_use_cases: open_edx
# .. toggle_creation_date: 2025-02-11
# .. toggle_warning: Only enable for Enterprise pilots; SAML/TPA flows remain on legacy.
# Gating flag for Enterprise AuthN MFE rollout
ENABLE_ENTERPRISE_REDIRECT_TO_AUTHN = WaffleFlag(
    f'{WAFFLE_FLAG_NAMESPACE}.enable_enterprise_redirect_to_authn',
    __name__
)


def is_require_third_party_auth_enabled():
    # TODO: Replace function with SettingToggle when it is available.
    return getattr(settings, "ENABLE_REQUIRE_THIRD_PARTY_AUTH", False)


def should_redirect_to_authn_microfrontend():
    """
    Checks if login/registration should be done via MFE.
    """
    request = get_current_request()
    if request and request.GET.get('skip_authn_mfe'):
        return False
    return configuration_helpers.get_value(
        'ENABLE_AUTHN_MICROFRONTEND', settings.FEATURES.get('ENABLE_AUTHN_MICROFRONTEND')
    )


# .. toggle_name: ENABLE_AUTO_GENERATED_USERNAME
# .. toggle_implementation: DjangoSetting
# .. toggle_default: False
# .. toggle_description: Set to True to enable auto-generation of usernames.
# .. toggle_use_cases: open_edx
# .. toggle_creation_date: 2024-02-20
# .. toggle_warning: Changing this setting may affect user authentication, account management and discussions experience.


def is_auto_generated_username_enabled():
    """
    Checks if auto-generated username should be enabled.
    """
    return configuration_helpers.get_value(
        'ENABLE_AUTO_GENERATED_USERNAME', settings.FEATURES.get('ENABLE_AUTO_GENERATED_USERNAME')
    )
