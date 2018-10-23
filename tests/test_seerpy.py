# Copyright 2017,2018 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

import json
import pathlib
from unittest import mock

import pytest
import pandas as pd

from seerpy.seerpy import SeerConnect


# having a class is useful to allow patches to be shared across mutliple test functions, but then
# pylint complains that the methods could be a function. this disables that warning.
# pylint:disable=no-self-use


@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestSeerConnect:

    def test_success(self, seer_auth):
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        result = SeerConnect()

        assert result.graphqlClient

    def test_login_unauthorized(self, seer_auth):
        seer_auth.return_value.cookie = None

        # not really desired behaviour, just documenting current behaviour
        with pytest.raises(AttributeError):
            SeerConnect()

    def test_login_error(self, seer_auth):
        seer_auth.side_effect = InterruptedError('Authentication Failed')

        with pytest.raises(InterruptedError):
            SeerConnect()


@mock.patch.object(SeerConnect, "__init__", autospec=True, return_value=None)
class TestCreateMetaData:

    # as we don't rely on anything in __init() I have mocked it for simplicity

    @mock.patch.object(SeerConnect, "getAllMetaData", autospec=True)
    def test_single_study(self, get_all_metadata,
                          seer_connect_init):  # pylint:disable=unused-argument

        # setup
        parent_dir = pathlib.Path(__file__).parent

        with open(parent_dir / "test_data/study1_metadata.json", "r") as f:
            test_input = json.load(f)
        studies = {'studies': [test_input['study']]}
        get_all_metadata.return_value = studies

        test_result = pd.read_csv(parent_dir / "test_data/study1_metadata.csv", index_col=0)

        # run test
        result = SeerConnect().createMetaData()

        # check result
        assert result.equals(test_result)

    @mock.patch.object(SeerConnect, "getAllMetaData", autospec=True)
    def test_four_studies(self, get_all_metadata,
                          seer_connect_init):  # pylint:disable=unused-argument

        # setup
        parent_dir = pathlib.Path(__file__).parent

        study_list = list()

        with open(parent_dir / "test_data/study1_metadata.json", "r") as f:
            study = json.load(f)
        study_list.append(study['study'])

        with open(parent_dir / "test_data/study2_metadata.json", "r") as f:
            study = json.load(f)
        study_list.append(study['study'])

        with open(parent_dir / "test_data/study3_metadata.json", "r") as f:
            study = json.load(f)
        study_list.append(study['study'])

        with open(parent_dir / "test_data/study4_metadata.json", "r") as f:
            study = json.load(f)
        study_list.append(study['study'])

        get_all_metadata.return_value = {'studies': study_list}

        test_result = pd.read_csv(parent_dir / "test_data/studies1-4_metadata.csv", index_col=0)

        # run test
        result = SeerConnect().createMetaData()

        # check result
        assert result.equals(test_result)
