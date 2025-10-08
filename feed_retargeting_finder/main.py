from auchan_ret_script import RemarketingFeedMatch
from utils import (
    file_save,
)
from constants import (
    FEED_DEEP,
)


def main():
    ex = RemarketingFeedMatch()
    ex.tree_raise_status()
    ex.feed_to_dataframe()
    ex.filter_has_matches()
    ex.apply_pair()
    ex.final_view()
    file_save(ex.df, True)


if __name__ == '__main__':
    main()
