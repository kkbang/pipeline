from .cocoapods import CocoaPodsAdapter
from .cpan import CPANAdapter
from .cratesio import CratesIOAdapter
from .gomod import GoModuleAdapter
from .hackage import HackageAdapter
from .hexpm import HexPMAdapter
from .maven import MavenCentralAdapter
from .npm import NPMAdapter
from .nuget import NuGetAdapter
from .packagist import PackagistAdapter
from .pubdev import PubDevAdapter
from .pypi import PyPIAdapter
from .rubygems import RubyGemsAdapter
from .swiftpm import SwiftPMAdapter

__all__ = [
    "CocoaPodsAdapter",
    "CPANAdapter",
    "CratesIOAdapter",
    "GoModuleAdapter",
    "HackageAdapter",
    "HexPMAdapter",
    "MavenCentralAdapter",
    "NPMAdapter",
    "NuGetAdapter",
    "PackagistAdapter",
    "PubDevAdapter",
    "PyPIAdapter",
    "RubyGemsAdapter",
    "SwiftPMAdapter",
]
