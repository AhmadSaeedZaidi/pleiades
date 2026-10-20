import asyncio
import sys

from atlas.setup import provision_schema


def main() -> int:
    try:
        asyncio.run(provision_schema())
        return 0
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
