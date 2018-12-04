from urllib.request import urlopen
from pandas import DataFrame, read_csv, concat

PROVINCES   =   {'Cherkasy': 1,
                 'Chernihiv': 2,
                 'Chernivtsi': 3,
                 'Crimea': 4,
                 'Dnipropetrovsk': 5,
                 'Donetsk': 6,
                 'Ivano-Frankivsk': 7,
                 'Kharkiv': 8,
                 'Kherson': 9,
                 'Khmelnytskyy': 10,
                 'Kiev': 11,
                 'Kiev City': 12,
                 'Kirovohrad': 13,
                 'Luhansk': 14,
                 'Lviv': 15,
                 'Mykolayiv': 16,
                 'Odessa': 17,
                 'Poltava': 18,
                 'Rivne': 19,
                 'Sevastopol': 20,
                 'Sumy': 21,
                 'Ternopil': 22,
                 'Transcarpathia': 23,
                 'Vinnytsya': 24,
                 'Volyn': 25,
                 'Zaporizhzhya': 26,
                 'Zhytomyr': 27}


def download_data(province_id):
    url1 = "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/get_provinceData.php?country=UKR&" \
          + "provinceID=" + str(province_id) \
          + "&year1=1981&year2=2017&type=Mean"

    # url2 = "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/get_provinceData.php?country=UKR&" \
    #       + "provinceID=" + str(province_id) \
    #       + "&year1=1981&year2=2017&type=VHI_Parea"

    from datetime import datetime as dt
    fname1 = 'data1_{}_{}'.format(dt.now().date(), province_id)
    # fname2 = 'data2_{}_{}'.format(dt.now().date(), province_id)

    def fix_csv(d):
        dd = d.read().decode().split('<pre>')[1].split('</pre')[0]
        dd = dd.replace(', provinceID, ', '')
        lines = dd.split('\n')
        res = [lines[0] + ',region']
        for l in lines[1:]:
            ll = l.replace(',', ' ')
            la = ll.split(' ')
            lx = ','.join([x for x in la if x]) + ',{}'.format(province_id)
            res.append(lx)
        return '\n'.join(res)

    try:
        open(fname1, 'r')
        # open(fname2, 'r')
    except FileNotFoundError:
        d1 = urlopen(url1)  # , urlopen(url2)
        with open(fname1, 'w') as f:
            f.write(fix_csv(d1))
        # with open(fname2, 'w') as f:
        #     f.write(fix_csv(d2))

    return fname1,  # fname2


def download_all_data():
    all_files = []
    for p_name, p_id in PROVINCES.items():
        t = download_data(p_id)
        all_files += list(t)
    return all_files


def get_frames():
    return [read_csv(x) for x in download_all_data()]


def get_extremums(df, year, region):
    r = PROVINCES[region]
    sel = df.loc[(df['year'] == year) & (df['region'] == r)]
    mx = sel.loc[sel['VHI'].idxmax()]
    mn = sel.loc[sel['VHI'].idxmin()]
    return {'max': mx, 'min': mn}


def get_with_vhi_less(df, region, vhi):
    r = PROVINCES[region]
    sel = df.loc[(df['region'] == r) & (df['VHI'] < vhi)]
    return sel


def main():
    frames = get_frames()
    df = concat(frames)

    max_min = get_extremums(df, 1986, 'Kiev')
    less = get_with_vhi_less(df, 'Kiev', 15.0)  # get all entries with vhi < 15.0

    print(max_min)
    print(less)

if __name__ == '__main__':
    main()