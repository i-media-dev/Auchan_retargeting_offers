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
    ex.feed_to_dataframe(FEED_DEEP)
    ex.filter_has_matches()
    ex.apply_pair()
    ex.final_view()
    # a = pd.DataFrame
    # ex._check_dataframe_exist(a)
    file_save(ex.df, False)


if __name__ == '__main__':
    main()
